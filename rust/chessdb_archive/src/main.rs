// Production lossless archive writer and native logical auditor. Five-field CDBARC02 protocol.
// CLI: INPUT|- OUTPUT.parquet ZSTD_LEVEL MAX_ROWS
use anyhow::{ensure, Context, Result};
use arrow::array::{Array, BinaryArray, StringArray, Int32Array, ListArray, StructArray, ArrayRef, BinaryBuilder, StringBuilder, Int32Builder, StructBuilder, ListBuilder};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use sha2::{Sha256,Digest};
use std::{fs::{File,OpenOptions},io::{Read,BufRead,BufReader},sync::Arc,time::Instant,path::PathBuf};
const BATCH:usize=32768;
fn blob(r:&mut impl Read)->Result<Vec<u8>> {
    let mut len=[0u8;4];r.read_exact(&mut len).context("truncated frame length")?;
    let n=u32::from_le_bytes(len) as usize;ensure!(n<=16*1024*1024,"oversize frame blob");
    let mut v=vec![0;n];r.read_exact(&mut v).context("truncated frame value")?;Ok(v)
}
fn source_hash(h:&mut Sha256,v:&[u8]) {h.update((v.len() as u32).to_le_bytes());h.update(v);}
fn write_archive(a:&[String])->Result<()> {ensure!(a.len()==5,"INPUT|- OUTPUT LEVEL MAX_ROWS");
    let started=Instant::now();let level:i32=a[3].parse()?;let expected:u64=a[4].parse()?;
    let mut input:BufReader<Box<dyn Read>>=BufReader::with_capacity(8<<20,if a[1]=="-" {Box::new(std::io::stdin())}else{Box::new(File::open(&a[1])?)});
    let mut magic=[0;8];input.read_exact(&mut magic)?;ensure!(&magic==b"CDBARC02","bad frame header");
    let out=PathBuf::from(&a[2]);ensure!(!out.exists(),"output already exists");
    let tmp=out.with_extension("parquet.tmp");let file=OpenOptions::new().write(true).create_new(true).open(&tmp)?;
    let move_fields=vec![Field::new("uci",DataType::Utf8,true),Field::new("score",DataType::Int32,true)];
    let schema=Arc::new(Schema::new(vec![Field::new("source_key",DataType::Binary,true),Field::new("source_value",DataType::Binary,true),Field::new("fen",DataType::Utf8,true),
      Field::new("moves",DataType::List(Arc::new(Field::new("item",DataType::Struct(move_fields.clone().into()),true))),true),Field::new("cdb_ply",DataType::Int32,true),Field::new("cdb_mirror_ply",DataType::Int32,true)]));
    let props=WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::try_new(level)?))
      .set_dictionary_enabled(false).set_max_row_group_row_count(Some(BATCH))
      .set_column_encoding(ColumnPath::from("source_key"),Encoding::DELTA_BYTE_ARRAY)
      .set_column_encoding(ColumnPath::from("fen"),Encoding::DELTA_BYTE_ARRAY).build();
    let mut writer=ArrowWriter::try_new(file,schema.clone(),Some(props))?;
    let mut count=0u64;let mut hash=Sha256::new();let mut stream=Sha256::new();let mut first=Vec::new();let mut last=Vec::new();
    loop {
      let mut kb=BinaryBuilder::new();let mut vb=BinaryBuilder::new();let mut fb=StringBuilder::new();let mut pb=Int32Builder::new();let mut mpb=Int32Builder::new();
      let mut mb=ListBuilder::new(StructBuilder::from_fields(move_fields.clone(),BATCH*3));let mut n=0;
      while n<BATCH && !input.fill_buf()?.is_empty() {
        let k=blob(&mut input)?;let v=blob(&mut input)?;let f=blob(&mut input)?;let m=blob(&mut input)?;let mp=blob(&mut input)?;
        for value in [&k,&v,&f,&m,&mp] {source_hash(&mut stream,value);}
        if count==0 {first=k.clone();}else{ensure!(last<k,"nonmonotone keys");}last=k.clone();source_hash(&mut hash,&k);source_hash(&mut hash,&v);
        kb.append_value(k);vb.append_value(v);
        if f.is_empty(){fb.append_null()}else{fb.append_value(std::str::from_utf8(&f)?)}
        let mut ply=None;
        for item in std::str::from_utf8(&m)?.split(';').filter(|s|!s.is_empty()) {
          let (uci,score)=item.split_once(':').context("bad decoded move")?;let score:i32=score.parse()?;
          if uci=="a0a0" {ensure!(ply.is_none(),"duplicate ply sentinel");ply=Some(score);}else{
            let sb=mb.values();sb.field_builder::<StringBuilder>(0).unwrap().append_value(uci);
            sb.field_builder::<Int32Builder>(1).unwrap().append_value(score);sb.append(true);
          }
        }
        mb.append(true);pb.append_option(ply);mpb.append_option(if mp.is_empty(){None}else{Some(std::str::from_utf8(&mp)?.parse::<i32>()?)});n+=1;count+=1;ensure!(count<=expected,"too many records");
      }
      if n==0 {break;}
      let arrays:Vec<ArrayRef>=vec![Arc::new(kb.finish()),Arc::new(vb.finish()),Arc::new(fb.finish()),Arc::new(mb.finish()),Arc::new(pb.finish()),Arc::new(mpb.finish())];
      writer.write(&RecordBatch::try_new(schema.clone(),arrays)?)?;
    }
    ensure!(count>0,"empty chunk");
    writer.close()?;OpenOptions::new().write(true).open(&tmp)?.sync_all()?;
    let elapsed=started.elapsed().as_secs_f64();let bytes=std::fs::metadata(&tmp)?.len();std::fs::rename(&tmp,&out)?;
    let hex=|v:&[u8]|v.iter().map(|b|format!("{b:02x}")).collect::<String>();
    let result=serde_json::json!({"records":count,"bytes":bytes,"seconds":elapsed,"source_logical_sha256":format!("{:x}",hash.finalize()),"stream_sha256":format!("{:x}",stream.finalize()),"first_key_hex":hex(&first),"last_key_hex":hex(&last),"zstd":level});
    let receipt=out.with_extension("parquet.receipt.json");let mut rf=OpenOptions::new().write(true).create_new(true).open(receipt)?;
    serde_json::to_writer_pretty(&mut rf,&result)?;rf.sync_all()?;println!("{result}");Ok(())
}

fn audit(path:&str)->Result<()> {
    let start=Instant::now();let mut raw=Sha256::new();let mut stream=Sha256::new();let mut count=0u64;let mut first=Vec::new();let mut last=Vec::new();
    let reader=ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?.with_batch_size(BATCH).build()?;
    for b in reader {let b=b?;
      ensure!(b.num_columns()==6,"wrong archive column count");
      for (i,name) in ["source_key","source_value","fen","moves","cdb_ply","cdb_mirror_ply"].iter().enumerate(){ensure!(b.schema().field(i).name()==name,"wrong column name");}
      let k=b.column(0).as_any().downcast_ref::<BinaryArray>().context("key type")?;let v=b.column(1).as_any().downcast_ref::<BinaryArray>().context("value type")?;
      let f=b.column(2).as_any().downcast_ref::<StringArray>().context("fen type")?;let m=b.column(3).as_any().downcast_ref::<ListArray>().context("moves type")?;
      let p=b.column(4).as_any().downcast_ref::<Int32Array>().context("ply type")?;let mp=b.column(5).as_any().downcast_ref::<Int32Array>().context("mirror ply type")?;
      for i in 0..b.num_rows(){ensure!(!k.is_null(i)&&!v.is_null(i)&&!m.is_null(i),"null authoritative fields");let key=k.value(i);let val=v.value(i);
        if count==0{first=key.to_vec();}else{ensure!(last.as_slice()<key,"nonmonotone archive keys");}last=key.to_vec();
        source_hash(&mut raw,key);source_hash(&mut raw,val);source_hash(&mut stream,key);source_hash(&mut stream,val);
        source_hash(&mut stream,if f.is_null(i){b""}else{f.value(i).as_bytes()});
        let moves=m.value(i);let moves=moves.as_any().downcast_ref::<StructArray>().context("moves struct")?;
        let u=moves.column(0).as_any().downcast_ref::<StringArray>().context("uci type")?;let s=moves.column(1).as_any().downcast_ref::<Int32Array>().context("score type")?;
        let mut text=String::new();for j in 0..moves.len(){ensure!(!moves.is_null(j)&&!u.is_null(j)&&!s.is_null(j),"null move");text.push_str(u.value(j));text.push(':');text.push_str(&s.value(j).to_string());text.push(';');}
        if !p.is_null(i){text.push_str("a0a0:");text.push_str(&p.value(i).to_string());text.push(';');}
        source_hash(&mut stream,text.as_bytes());source_hash(&mut stream,if mp.is_null(i){String::new()}else{mp.value(i).to_string()}.as_bytes());count+=1;
      }
    }
    let mut fh=Sha256::new();let mut file=File::open(path)?;let mut buf=vec![0;8<<20];loop{let n=file.read(&mut buf)?;if n==0{break;}fh.update(&buf[..n]);}
    let hex=|v:&[u8]|v.iter().map(|b|format!("{b:02x}")).collect::<String>();
    println!("{}",serde_json::json!({"records":count,"source_logical_sha256":format!("{:x}",raw.finalize()),"stream_sha256":format!("{:x}",stream.finalize()),"first_key_hex":hex(&first),"last_key_hex":hex(&last),"file_sha256":format!("{:x}",fh.finalize()),"bytes":std::fs::metadata(path)?.len(),"seconds":start.elapsed().as_secs_f64()}));Ok(())
}
fn main()->Result<()> {
 let a:Vec<String>=std::env::args().collect();ensure!(a.len()>=3,"write INPUT OUTPUT LEVEL MAX_ROWS | audit FILE");
 match a[1].as_str(){"write"=>{let mut w=vec![a[0].clone()];w.extend_from_slice(&a[2..]);write_archive(&w)},"audit"=>{ensure!(a.len()==3,"audit FILE");audit(&a[2])},_=>anyhow::bail!("unknown command")}
}
