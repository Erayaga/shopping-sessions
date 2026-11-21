//! Output: Parquet (scored) and CSV (for diffing against the reference).

use std::fs::{self, File};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;

use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;

use crate::parse::civil_from_days;
use crate::{Corpus, URow};

pub struct Ctx<'a> {
    pub corpus: &'static Corpus,
    pub cats: &'static [Vec<u8>],
    pub campaigns: &'a [Vec<Vec<u8>>],
    pub organic: u32,
}

impl Ctx<'_> {
    fn camp_id(&self, c: u32) -> &[u8] {
        if c == self.organic {
            b"organic"
        } else {
            &self.campaigns[c as usize][0]
        }
    }
    fn channel(&self, c: u32) -> &[u8] {
        if c == self.organic {
            b"organic"
        } else {
            &self.campaigns[c as usize][1]
        }
    }
}

fn day_str(d: i32) -> String {
    let (y, m, dd) = civil_from_days(d as i64);
    format!("{y:04}-{m:02}-{dd:02}")
}

pub fn write_csv(out: &Path, ctx: &Ctx, users: &[Vec<URow>], camps: &[(u32, i32, [u64; 4])]) {
    fs::create_dir_all(out).unwrap();
    let mut w = BufWriter::new(File::create(out.join("user_daily.csv")).unwrap());
    w.write_all(b"user_id,day,sessions,events,revenue_cents,top_category\n").unwrap();
    for part in users {
        for r in part {
            w.write_all(ctx.corpus.bytes(r.name)).unwrap();
            write!(w, ",{},{},{},{},", day_str(r.day), r.sessions, r.events, r.revenue).unwrap();
            if r.top != u16::MAX {
                w.write_all(&ctx.cats[r.top as usize]).unwrap();
            }
            w.write_all(b"\n").unwrap();
        }
    }
    w.flush().unwrap();
    let mut w = BufWriter::new(File::create(out.join("campaign_daily.csv")).unwrap());
    w.write_all(b"campaign_id,channel,day,sessions,purchasing_sessions,revenue_cents,assisted_sessions\n").unwrap();
    for &(c, d, v) in camps {
        w.write_all(ctx.camp_id(c)).unwrap();
        w.write_all(b",").unwrap();
        w.write_all(ctx.channel(c)).unwrap();
        writeln!(w, ",{},{},{},{},{}", day_str(d), v[0], v[1], v[2], v[3]).unwrap();
    }
    w.flush().unwrap();
}

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .set_max_row_group_size(1 << 30)
            .build(),
    )
}

const USER_SCHEMA: &str = "message user_daily {
    REQUIRED BYTE_ARRAY user_id (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 events;
    REQUIRED INT64 revenue_cents;
    OPTIONAL BYTE_ARRAY top_category (UTF8);
}";

const CAMP_SCHEMA: &str = "message campaign_daily {
    REQUIRED BYTE_ARRAY campaign_id (UTF8);
    REQUIRED BYTE_ARRAY channel (UTF8);
    REQUIRED INT32 day (DATE);
    REQUIRED INT64 sessions;
    REQUIRED INT64 purchasing_sessions;
    REQUIRED INT64 revenue_cents;
    REQUIRED INT64 assisted_sessions;
}";

enum Col {
    Bytes(Vec<ByteArray>, Option<Vec<i16>>),
    I32(Vec<i32>),
    I64(Vec<i64>),
}

fn write_file(path: &Path, schema: &str, cols: Vec<Col>) {
    let schema = Arc::new(parse_message_type(schema).unwrap());
    let file = File::create(path).unwrap();
    let mut writer = SerializedFileWriter::new(BufWriter::with_capacity(1 << 20, file), schema, props()).unwrap();
    let mut rg = writer.next_row_group().unwrap();
    let mut cols = cols.into_iter();
    while let Some(mut col) = rg.next_column().unwrap() {
        match cols.next().unwrap() {
            Col::Bytes(v, def) => {
                col.typed::<ByteArrayType>().write_batch(&v, def.as_deref(), None).unwrap();
            }
            Col::I32(v) => {
                col.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
            }
            Col::I64(v) => {
                col.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            }
        }
        col.close().unwrap();
    }
    rg.close().unwrap();
    writer.close().unwrap();
}

/// Writes one thread's user_daily rows as dir/part-NNN.parquet.
pub fn write_user_part(dir: &Path, n: usize, ctx: &Ctx, part: &[URow]) {
    let mut top = Vec::new();
    let mut def = Vec::with_capacity(part.len());
    for r in part {
        if r.top == u16::MAX {
            def.push(0i16);
        } else {
            def.push(1);
            top.push(ByteArray::from(Bytes::from_static(&ctx.cats[r.top as usize])));
        }
    }
    let cols = vec![
        Col::Bytes(part.iter().map(|r| ByteArray::from(Bytes::from_static(ctx.corpus.bytes(r.name)))).collect(), None),
        Col::I32(part.iter().map(|r| r.day).collect()),
        Col::I64(part.iter().map(|r| r.sessions as i64).collect()),
        Col::I64(part.iter().map(|r| r.events as i64).collect()),
        Col::I64(part.iter().map(|r| r.revenue as i64).collect()),
        Col::Bytes(top, Some(def)),
    ];
    write_file(&dir.join(format!("part-{n:03}.parquet")), USER_SCHEMA, cols);
}

pub fn write_campaigns(dir: &Path, ctx: &Ctx, camps: &[(u32, i32, [u64; 4])]) {
    let cols = vec![
        Col::Bytes(camps.iter().map(|c| ByteArray::from(ctx.camp_id(c.0).to_vec())).collect(), None),
        Col::Bytes(camps.iter().map(|c| ByteArray::from(ctx.channel(c.0).to_vec())).collect(), None),
        Col::I32(camps.iter().map(|c| c.1).collect()),
        Col::I64(camps.iter().map(|c| c.2[0] as i64).collect()),
        Col::I64(camps.iter().map(|c| c.2[1] as i64).collect()),
        Col::I64(camps.iter().map(|c| c.2[2] as i64).collect()),
        Col::I64(camps.iter().map(|c| c.2[3] as i64).collect()),
    ];
    write_file(&dir.join("part-000.parquet"), CAMP_SCHEMA, cols);
}
