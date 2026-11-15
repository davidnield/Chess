//! Lossless physical layout for book-v4 move slices. Monthly and derived schemas are independent.
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, ArrayRef, AsArray, Int64Array, RecordBatch, StringBuilder};
use arrow::datatypes::{DataType, Field, Int64Type, Schema, SchemaRef};
use parquet::arrow::{arrow_reader::ParquetRecordBatchReaderBuilder, ProjectionMask};
use std::{fs::File, path::Path, sync::Arc};

const RANKS: [&str; 8] = [
    "rank8", "rank7", "rank6", "rank5", "rank4", "rank3", "rank2", "rank1",
];
const EPD: [&str; 9] = [
    "rank8",
    "rank7",
    "rank6",
    "rank5",
    "rank4",
    "rank3",
    "rank2",
    "rank1",
    "epd_state",
];

pub(crate) fn physical_schema(logical: &SchemaRef) -> SchemaRef {
    let mut fields: Vec<_> = logical
        .fields()
        .iter()
        .filter(|f| {
            !matches!(
                f.name().as_str(),
                "parent_epd" | "child_hash" | "child_hash_hi"
            )
        })
        .cloned()
        .collect();
    fields.extend(
        EPD.iter()
            .map(|name| Arc::new(Field::new(*name, DataType::Utf8, false))),
    );
    for name in ["move_xor_lo", "move_xor_hi"] {
        fields.push(Arc::new(Field::new(name, DataType::Int64, false)));
    }
    Arc::new(Schema::new(fields))
}

fn col<'a>(b: &'a RecordBatch, name: &str) -> Result<&'a ArrayRef> {
    b.column_by_name(name)
        .ok_or_else(|| anyhow!("missing explorer column {name}"))
}

fn xor(b: &RecordBatch, first: &str, second: &str) -> Result<ArrayRef> {
    let a = col(b, first)?.as_primitive::<Int64Type>();
    let c = col(b, second)?.as_primitive::<Int64Type>();
    if a.null_count() + c.null_count() != 0 {
        bail!("NULL hash in {first}/{second}");
    }
    Ok(Arc::new(Int64Array::from_iter_values(
        a.values().iter().zip(c.values()).map(|(x, y)| x ^ y),
    )))
}

pub(crate) fn encode(b: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let mut cols: Vec<_> = schema
        .fields()
        .iter()
        .filter_map(|f| b.column_by_name(f.name()).cloned())
        .collect();
    let mut parts: [StringBuilder; 9] = std::array::from_fn(|_| StringBuilder::new());
    for value in col(b, "parent_epd")?.as_string::<i32>().iter() {
        let (board, state) = value
            .and_then(|v| v.split_once(' '))
            .context("incomplete parent EPD")?;
        let mut ranks = board.split('/');
        for rank in &mut parts[..8] {
            rank.append_value(ranks.next().context("too few EPD ranks")?);
        }
        if ranks.next().is_some() {
            bail!("too many EPD ranks");
        }
        parts[8].append_value(state);
    }
    cols.extend(
        parts
            .iter_mut()
            .map(|builder| Arc::new(builder.finish()) as ArrayRef),
    );
    cols.push(xor(b, "parent_hash", "child_hash")?);
    cols.push(xor(b, "parent_hash_hi", "child_hash_hi")?);
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

fn dependencies(name: &str) -> &[&str] {
    match name {
        "parent_epd" => &EPD,
        "child_hash" => &["parent_hash", "move_xor_lo"],
        "child_hash_hi" => &["parent_hash_hi", "move_xor_hi"],
        _ => &[],
    }
}

pub(crate) fn decode(b: &RecordBatch, schema: &SchemaRef) -> Result<RecordBatch> {
    let mut cols = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let arr: ArrayRef = match field.name().as_str() {
            "child_hash" => xor(b, "parent_hash", "move_xor_lo")?,
            "child_hash_hi" => xor(b, "parent_hash_hi", "move_xor_hi")?,
            "parent_epd" => {
                let parts = EPD
                    .iter()
                    .map(|name| Ok(col(b, name)?.as_string::<i32>()))
                    .collect::<Result<Vec<_>>>()?;
                if parts.iter().any(|p| p.null_count() != 0) {
                    bail!("NULL EPD component");
                }
                let mut out = StringBuilder::with_capacity(b.num_rows(), b.num_rows() * 65);
                let mut epd = String::with_capacity(96);
                for i in 0..b.num_rows() {
                    epd.clear();
                    for (j, part) in parts.iter().enumerate() {
                        if j != 0 {
                            epd.push(if j == RANKS.len() { ' ' } else { '/' });
                        }
                        epd.push_str(part.value(i));
                    }
                    out.append_value(&epd);
                }
                Arc::new(out.finish())
            }
            name => col(b, name)?.clone(),
        };
        cols.push(arr);
    }
    Ok(RecordBatch::try_new(schema.clone(), cols)?)
}

pub(crate) type Reader = Box<dyn Iterator<Item = Result<RecordBatch>> + Send>;
pub(crate) fn reader(path: &Path, cols: &[&str], batch: usize, compact: bool) -> Result<Reader> {
    let builder = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)
        .with_context(|| format!("opening {}", path.display()))?;
    if compact != builder.schema().index_of("move_xor_lo").is_ok() {
        bail!(
            "move slice disagrees with declared explorer layout at {}",
            path.display()
        );
    }
    let base = crate::merge::book_ps_schema_with(true);
    let schema = Arc::new(
        base.project(
            &cols
                .iter()
                .map(|c| base.index_of(c))
                .collect::<Result<Vec<_>, _>>()?,
        )?,
    );
    let mut idx = Vec::new();
    for &name in cols {
        let deps = dependencies(name);
        if compact && !deps.is_empty() {
            for dep in deps {
                idx.push(builder.schema().index_of(dep)?);
            }
        } else {
            idx.push(builder.schema().index_of(name)?);
        }
    }
    let mask = ProjectionMask::roots(builder.parquet_schema(), idx);
    let rdr = builder
        .with_projection(mask)
        .with_batch_size(batch)
        .build()?;
    Ok(Box::new(rdr.map(move |b| {
        let b = b?;
        if compact {
            decode(&b, &schema)
        } else {
            Ok(b)
        }
    })))
}
