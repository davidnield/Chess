//! Frozen standard-start OTB scores -> legal 128-bit decision records.
//! No source offsets, game identities, rating filters, or partitions are inferred here.
use shakmaty::{Chess, Position, EnPassantMode, CastlingMode, fen::Epd, san::SanPlus, zobrist::Zobrist128};
use std::slice;
#[path = "../../explorer_extract/src/san.rs"] mod san;
#[path = "../../explorer_extract/src/pyre.rs"] mod pyre;
#[path = "../../explorer_extract/src/pydigits.rs"] mod pydigits;

#[repr(C)]
#[derive(Clone, Copy)]
pub struct Edge {
    pub parent_lo:i64,pub parent_hi:i64,pub child_lo:i64,pub child_hi:i64,
    pub san:[u8;12],pub uci:[u8;8],pub epd:[u8;96],
}
#[no_mangle] pub extern "C" fn edge_size()->usize { std::mem::size_of::<Edge>() }

// Stop at the requested prefix or a mainline result. Variations never contribute.
// Bad syntax before the horizon is rejected, not silently deleted.
fn tokens(text:&str, cap:usize)->Result<Vec<String>, i32> {
    let b=text.as_bytes(); let(mut i,mut depth)=(0,0usize);let mut out=Vec::new();
    while i<b.len() && out.len()<cap {
        match b[i] {
            b'{' => {i+=1;while i<b.len() && b[i]!=b'}'{i+=1;}if i==b.len(){return Err(-1);}i+=1;},
            b';'|b'%' => {while i<b.len() && b[i]!=b'\n'{i+=1;}},
            b'(' => {depth+=1;i+=1;},
            b')' => {if depth==0{return Err(-1);}depth-=1;i+=1;},
            b'}'|b'['|b']' => return Err(-1),
            b'$' => {i+=1;let start=i;while i<b.len() && b[i].is_ascii_digit(){i+=1;}if i==start{return Err(-1);}},
            c if c.is_ascii_whitespace() => {i+=1;},
            _ => {
                let start=i;i+=1;while i<b.len() && !b[i].is_ascii_whitespace() && !b"{}();%$[]".contains(&b[i]){i+=1;}
                if depth>0{continue;}
                let mut t=&text[start..i];
                let digits=t.bytes().take_while(|x|x.is_ascii_digit()).count();
                if t.as_bytes().get(digits)==Some(&b'.'){t=t[digits..].trim_start_matches('.');}
                t=t.trim_end_matches(['!','?']);
                if matches!(t,"1-0"|"0-1"|"1/2-1/2"|"*"){break;}
                if matches!(t,""|"..."|"e.p."|"ep"){continue;}
                out.push(t.to_string());
            }
        }
    }
    if depth!=0{return Err(-1);}Ok(out)
}
fn put<const N:usize>(s:&str)->[u8;N]{let mut a=[0;N];assert!(s.len()<N);a[..s.len()].copy_from_slice(s.as_bytes());a}
fn key(b:&Chess)->(i64,i64){let z:Zobrist128=b.zobrist_hash(EnPassantMode::PseudoLegal);(z.0 as i64,(z.0>>64) as i64)}
fn replay(text:&str,cap:usize,dest:&mut [Edge])->i32 {
    let ts=match tokens(text,cap){Ok(x)=>x,Err(x)=>return x};if ts.is_empty(){return -4;}
    let mut b=Chess::default();
    for (i,t) in ts.iter().enumerate(){
        let m=match san::parse_san(&b,t){Ok(san::Parsed::Move(m))=>m,Ok(san::Parsed::Null)=>return -3,Err(_)=>return -2};
        let (lo,hi)=key(&b);let epd=Epd::from_position(&b,EnPassantMode::PseudoLegal).to_string();
        let uci=m.to_uci(CastlingMode::Standard).to_string();
        let canonical=SanPlus::from_move(b.clone(),m).to_string();
        b.play_unchecked(m);let(clo,chi)=key(&b);
        dest[i]=Edge{parent_lo:lo,parent_hi:hi,child_lo:clo,child_hi:chi,san:put(&canonical),uci:put(&uci),epd:put(&epd)};
    }
    ts.len() as i32
}
/// Buffers are caller-owned Arrow UTF-8 and contiguous Edge[n*cap], lengths[n].
/// A negative length invalidates the ENTIRE requested prefix; partial writes are ignored.
#[no_mangle]
pub unsafe extern "C" fn replay_batch(raw:*const u8, byte_len:usize, offsets:*const i64,n:usize,cap:usize,out:*mut Edge,lengths:*mut i32)->i32 {
    if cap==0 || cap>100 || n>65536{return -9;}
    let bytes=slice::from_raw_parts(raw,byte_len);let bounds=slice::from_raw_parts(offsets,n+1);
    let rows=slice::from_raw_parts_mut(out,n*cap);let lens=slice::from_raw_parts_mut(lengths,n);
    for i in 0..n {
        let Some(src)=bytes.get(bounds[i] as usize..bounds[i+1] as usize)else{return -9;};
        let Ok(text)=std::str::from_utf8(src)else{lens[i]=-1;continue;};
        lens[i]=replay(text,cap,&mut rows[i*cap..(i+1)*cap]);
    }
    0
}
#[cfg(test)] mod tests {
    use super::*;
    #[test]fn comments_variations_results(){assert_eq!(tokens("{url}\n\n1.e4 (1.d4 (1.c4)) e5 $1 2.Nf3!? Nc6 1-0 3.Bc4",51).unwrap(),["e4","e5","Nf3","Nc6"]);}
    #[test]fn syntax_and_prefix(){assert!(tokens("1.e4 {unfinished",51).is_err());assert_eq!(tokens("1.e4 {unfinished",1).unwrap(),["e4"]);assert!(tokens("1.e4 )",51).is_err());}
}
