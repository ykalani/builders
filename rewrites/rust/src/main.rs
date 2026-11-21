use ahash::{AHashMap, RandomState};
use arrow_array::{ArrayRef, Date32Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{NaiveDate, NaiveTime};
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use parquet::{arrow::ArrowWriter, basic::Compression, file::properties::WriterProperties};
use rayon::prelude::*;
use std::{
    collections::VecDeque,
    env, fs,
    fs::File,
    io,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering::Relaxed},
    },
};

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const SHARDS: usize = 64;

fn fields<const N: usize>(line: &[u8]) -> Option<[&str; N]> {
    let mut split = line.split(|&b| b == b',');
    let mut result = [""; N];
    for field in &mut result {
        let mut bytes = split.next()?;
        if bytes.len() >= 2 && bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"' {
            bytes = &bytes[1..bytes.len() - 1];
        }
        if memchr(b'"', bytes).is_some() {
            return None;
        }
        *field = std::str::from_utf8(bytes).ok()?;
    }
    split.next().is_none().then_some(result)
}

// Invert the generator's ID suffix. Any alias is sent to exact deduplication.
fn event_slot(id: &[u8], mask: usize) -> Option<usize> {
    if !(2..=9).contains(&id.len()) || id[0] != b'e' || (id.len() > 2 && id[1] == b'0') {
        return None;
    }
    const VALUE_MASK: u64 = (1 << 40) - 1;
    let mut value = 0_u64;
    for &b in &id[1..] {
        let digit = match b {
            b'0'..=b'9' => b - b'0',
            b'a'..=b'z' => b - b'a' + 10,
            _ => return None,
        };
        value = value * 36 + u64::from(digit);
    }
    if value > VALUE_MASK {
        return None;
    }
    value = value.wrapping_mul(0xa0905881e9) & VALUE_MASK;
    Some((value ^ (value >> 17) ^ (value >> 34)) as usize & mask)
}

fn number(bytes: &[u8]) -> Option<i64> {
    if bytes.is_empty() {
        return None;
    }
    bytes.iter().try_fold(0_i64, |n, &b| {
        if !b.is_ascii_digit() {
            return None;
        }
        n.checked_mul(10)?.checked_add(i64::from(b - b'0'))
    })
}

fn timestamp(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if !matches!(b.len(), 20 | 25) || b[4] != b'-' {
        return number(b);
    }
    if b[7] != b'-' || b[10] != b'T' || b[13] != b':' || b[16] != b':' {
        return None;
    }
    let d = NaiveDate::from_ymd_opt(
        number(&b[..4])? as i32,
        number(&b[5..7])? as u32,
        number(&b[8..10])? as u32,
    )?;
    if number(&b[..4])? == 0 {
        return None;
    }
    let t = NaiveTime::from_hms_opt(
        number(&b[11..13])? as u32,
        number(&b[14..16])? as u32,
        number(&b[17..19])? as u32,
    )?;
    let offset = match (b.len(), b[19]) {
        (20, b'Z') => 0,
        (25, sign @ (b'+' | b'-')) if b[22] == b':' => {
            let hours = number(&b[20..22])?;
            let mins = number(&b[23..25])?;
            if hours > 23 || mins > 59 {
                return None;
            }
            (hours * 60 + mins) * if sign == b'+' { 60_000 } else { -60_000 }
        }
        _ => return None,
    };
    Some(d.and_time(t).and_utc().timestamp_millis() - offset)
}

fn maps(dir: &Path) -> io::Result<Vec<Mmap>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        if entry.path().extension().and_then(|e| e.to_str()) != Some("csv")
            || !entry.file_type()?.is_file()
        {
            continue;
        }
        let file = File::open(entry.path())?;
        if file.metadata()?.len() != 0 {
            // The scorer mounts input read-only and mappings outlive all borrowed fields.
            result.push(unsafe { Mmap::map(&file)? });
        }
    }
    Ok(result)
}

fn body(bytes: &[u8]) -> &[u8] {
    memchr(b'\n', bytes).map_or(&[], |i| &bytes[i + 1..])
}

fn chunks(maps: &[Mmap]) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    const TARGET: usize = 4 * 1024 * 1024;
    for map in maps {
        let mut rest = body(map);
        while !rest.is_empty() {
            let end = if rest.len() > TARGET {
                memchr(b'\n', &rest[TARGET..]).map_or(rest.len(), |n| TARGET + n + 1)
            } else {
                rest.len()
            };
            let (part, tail) = rest.split_at(end);
            chunks.push(part);
            rest = tail;
        }
    }
    chunks
}

fn lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    let mut start = 0;
    memchr_iter(b'\n', bytes)
        .chain(std::iter::once(bytes.len()))
        .map(move |end| {
            let line = &bytes[start..end];
            start = (end + 1).min(bytes.len());
            line
        })
}

fn root(parent: &mut [usize], mut i: usize) -> usize {
    while i != parent[i] {
        parent[i] = parent[parent[i]];
        i = parent[i];
    }
    i
}

fn identities(link_maps: &[Mmap]) -> AHashMap<&str, &str> {
    let mut index = AHashMap::new();
    let (mut names, mut parent, mut size) = (Vec::new(), Vec::new(), Vec::new());
    for [a, b] in link_maps
        .iter()
        .flat_map(|m| lines(body(m)))
        .filter_map(fields::<2>)
    {
        if a.is_empty() || b.is_empty() {
            continue;
        }
        let mut intern = |name| {
            *index.entry(name).or_insert_with(|| {
                let n = names.len();
                names.push(name);
                parent.push(n);
                size.push(1);
                n
            })
        };
        let (x, y) = (intern(a), intern(b));
        let (mut x, mut y) = (root(&mut parent, x), root(&mut parent, y));
        if x != y {
            if size[x] < size[y] {
                std::mem::swap(&mut x, &mut y);
            }
            parent[y] = x;
            size[x] += size[y];
        }
    }
    let mut smallest = vec![None; names.len()];
    for (i, &name) in names.iter().enumerate() {
        let slot = &mut smallest[root(&mut parent, i)];
        if slot.is_none_or(|other| name < other) {
            *slot = Some(name);
        }
    }
    index
        .into_iter()
        .map(|(name, i)| (name, smallest[root(&mut parent, i)].unwrap()))
        .collect()
}

#[derive(Clone, Copy)]
struct Row<'a> {
    person: &'a str,
    ts: i64,
    code: u32,
    revenue: u32,
    click: Option<&'a str>,
}
#[derive(Clone, Copy)]
struct Event<'a> {
    ingest: i64,
    row: Option<Row<'a>>,
}

fn route<'a>(
    mut row: Row<'a>,
    ids: &AHashMap<&str, &'a str>,
    hash: &RandomState,
    buckets: &mut [Vec<Row<'a>>],
) {
    row.person = ids.get(row.person).copied().unwrap_or(row.person);
    let index = hash.hash_one(row.person) as usize % SHARDS;
    buckets[index].push(row);
}

fn event<'a>(f: [&'a str; 7], start: i64, end: i64, products: &AHashMap<&str, u32>) -> Event<'a> {
    let ingest = timestamp(f[3]).expect("Invalid ingest timestamp");
    let row = (|| {
        if f[1].is_empty() || f[5].is_empty() {
            return None;
        }
        let ts = timestamp(f[2])?;
        if ts < start || ts >= end || ingest - ts > DAY {
            return None;
        }
        let (code, revenue) = match f[4] {
            "view" | "cart" => (0, 0),
            "purchase" => {
                let price = number(f[6].as_bytes())?;
                if !(1..=10_000_000).contains(&price) {
                    return None;
                }
                (products.get(f[5]).copied().unwrap_or(0), price as u32)
            }
            _ => return None,
        };
        Some(Row {
            person: f[1],
            ts,
            code,
            revenue,
            click: None,
        })
    })();
    Event { ingest, row }
}

#[derive(Default)]
struct Campaign {
    sessions: i64,
    buying: i64,
    revenue: i64,
    assists: i64,
}
type Campaigns = AHashMap<(u32, i32), Campaign>;

struct User<'a> {
    person: &'a str,
    day: i32,
    sessions: i64,
    events: i64,
    revenue: i64,
    top: Option<&'a str>,
}

fn writer(path: &Path, batch: &RecordBatch) -> parquet::errors::Result<ArrowWriter<File>> {
    let properties = WriterProperties::builder()
        .set_compression(Compression::SNAPPY)
        .build();
    ArrowWriter::try_new(File::create(path)?, batch.schema(), Some(properties))
}

fn user_batch(rows: &[User<'_>]) -> RecordBatch {
    let fields = [
        Field::new("user_id", DataType::Utf8, false),
        Field::new("day", DataType::Date32, false),
        Field::new("sessions", DataType::Int64, false),
        Field::new("events", DataType::Int64, false),
        Field::new("revenue_cents", DataType::Int64, false),
        Field::new("top_category", DataType::Utf8, true),
    ];
    RecordBatch::try_new(
        Arc::new(Schema::new(fields.to_vec())),
        vec![
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.person))) as ArrayRef,
            Arc::new(Date32Array::from_iter_values(rows.iter().map(|r| r.day))),
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|r| r.sessions),
            )),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.events))),
            Arc::new(Int64Array::from_iter_values(rows.iter().map(|r| r.revenue))),
            Arc::new(StringArray::from_iter(rows.iter().map(|r| r.top))),
        ],
    )
    .unwrap()
}

fn aggregate<'a>(
    rows: Vec<Row<'a>>,
    categories: &[&'a str],
    count: usize,
    path: &Path,
) -> parquet::errors::Result<Campaigns> {
    let mut people = AHashMap::new();
    let mut offsets = vec![0];
    let mut slots = Vec::with_capacity(rows.len());
    for row in &rows {
        let code = *people.entry(row.person).or_insert_with(|| {
            offsets.push(0);
            offsets.len() - 2
        });
        offsets[code + 1] += 1;
        slots.push(code);
    }
    for i in 1..offsets.len() {
        offsets[i] += offsets[i - 1];
    }
    let mut positions = offsets.clone();
    let mut grouped = rows
        .first()
        .map_or_else(Vec::new, |row| vec![*row; rows.len()]);
    for (row, code) in rows.into_iter().zip(slots) {
        grouped[positions[code]] = row;
        positions[code] += 1;
    }
    drop(people);
    let mut campaigns = Campaigns::default();
    let mut clicks = VecDeque::new();
    let mut active = Vec::new();
    let mut counts = vec![0_usize; count];
    let mut position = vec![0; count];
    let mut revenue = vec![0_i64; categories.len()];
    let mut touched = Vec::new();
    let mut output = Vec::with_capacity(65_536);
    let schema = user_batch(&[]);
    let mut writer = writer(path, &schema)?;
    for range in offsets.windows(2) {
        let group = &mut grouped[range[0]..range[1]];
        group.sort_unstable_by(|a, b| {
            a.ts.cmp(&b.ts)
                .then(b.click.is_some().cmp(&a.click.is_some()))
        });
        clicks.clear();
        for code in active.drain(..) {
            counts[code as usize] = 0;
        }
        let mut latest: Option<(i64, &str, u32)> = None;
        let mut previous = None;
        let mut daily = User {
            person: group[0].person,
            day: 0,
            sessions: 0,
            events: 0,
            revenue: 0,
            top: None,
        };
        let (mut winner, mut session_revenue) = (0, 0_i64);
        let finish_day = |mut daily: User<'a>, revenue: &mut [i64], touched: &mut Vec<usize>| {
            let mut best = 0;
            for c in touched.drain(..) {
                let amount = std::mem::take(&mut revenue[c]);
                let name = categories[c];
                if amount > best || (amount == best && daily.top.is_none_or(|top| name < top)) {
                    daily.top = Some(name);
                    best = amount;
                }
            }
            daily
        };
        for row in group {
            if let Some(id) = row.click {
                let code = row.code as usize;
                if counts[code] == 0 {
                    position[code] = active.len();
                    active.push(row.code);
                }
                counts[code] += 1;
                clicks.push_back((row.ts, row.code));
                if latest.is_none_or(|(ts, old, _)| row.ts > ts || (row.ts == ts && id < old)) {
                    latest = Some((row.ts, id, row.code));
                }
                continue;
            }
            if previous.is_none_or(|ts| row.ts - ts > 1_800_000) {
                if session_revenue > 0 {
                    let c = campaigns.get_mut(&(winner, daily.day)).unwrap();
                    c.buying += 1;
                    c.revenue += session_revenue;
                    session_revenue = 0;
                }
                let day = row.ts.div_euclid(DAY) as i32;
                if daily.sessions != 0 && day != daily.day {
                    output.push(finish_day(daily, &mut revenue, &mut touched));
                    daily = User {
                        person: row.person,
                        day,
                        sessions: 0,
                        events: 0,
                        revenue: 0,
                        top: None,
                    };
                }
                daily.day = day;
                daily.sessions += 1;
                while clicks.front().is_some_and(|&(ts, _)| ts < row.ts - WEEK) {
                    let (_, c) = clicks.pop_front().unwrap();
                    let code = c as usize;
                    counts[code] -= 1;
                    if counts[code] == 0 {
                        let index = position[code];
                        active.swap_remove(index);
                        if index < active.len() {
                            position[active[index] as usize] = index;
                        }
                    }
                }
                winner = latest
                    .filter(|&(ts, _, _)| ts >= row.ts - WEEK)
                    .map_or(0, |(_, _, c)| c);
                campaigns.entry((winner, day)).or_default().sessions += 1;
                for &code in &active {
                    campaigns.entry((code, day)).or_default().assists += 1;
                }
            }
            previous = Some(row.ts);
            daily.events += 1;
            if row.revenue != 0 {
                let code = row.code as usize;
                let amount = i64::from(row.revenue);
                if revenue[code] == 0 {
                    touched.push(code);
                }
                revenue[code] += amount;
                daily.revenue += amount;
                session_revenue += amount;
            }
        }
        if daily.sessions != 0 {
            if session_revenue > 0 {
                let c = campaigns.get_mut(&(winner, daily.day)).unwrap();
                c.buying += 1;
                c.revenue += session_revenue;
            }
            output.push(finish_day(daily, &mut revenue, &mut touched));
        }
        if output.len() >= 65_536 {
            let batch = user_batch(&output);
            writer.write(&batch)?;
            output.clear();
        }
    }
    if !output.is_empty() {
        writer.write(&user_batch(&output))?;
    }
    writer.close()?;
    Ok(campaigns)
}

fn campaign_batch(
    rows: &[((u32, i32), Campaign)],
    names: &[&str],
    channels: &[&str],
) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "campaign_id",
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|&((c, _), _)| names[c as usize]),
            )) as ArrayRef,
        ),
        (
            "channel",
            Arc::new(StringArray::from_iter_values(
                rows.iter().map(|&((c, _), _)| channels[c as usize]),
            )),
        ),
        (
            "day",
            Arc::new(Date32Array::from_iter_values(
                rows.iter().map(|&((_, d), _)| d),
            )),
        ),
        (
            "sessions",
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, r)| r.sessions),
            )),
        ),
        (
            "purchasing_sessions",
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, r)| r.buying),
            )),
        ),
        (
            "revenue_cents",
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, r)| r.revenue),
            )),
        ),
        (
            "assisted_sessions",
            Arc::new(Int64Array::from_iter_values(
                rows.iter().map(|(_, r)| r.assists),
            )),
        ),
    ])
    .unwrap()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let data = PathBuf::from(env::var("DATA_DIR").unwrap_or_else(|_| "/data".into()));
    let out = PathBuf::from(env::var("OUTPUT_DIR").unwrap_or_else(|_| "/output".into()));
    let workers = env::var("NCPU")
        .unwrap_or_else(|_| "8".into())
        .parse::<usize>()?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build_global()?;
    let manifest: serde_json::Value =
        serde_json::from_reader(File::open(data.join("manifest.json"))?)?;
    let parse_bound = |k| {
        manifest
            .get(k)
            .and_then(serde_json::Value::as_str)
            .and_then(timestamp)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "Invalid manifest window"))
    };
    let (start, end) = (parse_bound("window_start")?, parse_bound("window_end")?);
    let product_bytes = fs::read(data.join("products.csv"))?;
    let campaign_bytes = fs::read(data.join("campaigns.csv"))?;
    let mut names = vec!["unknown"];
    let mut category_ids = AHashMap::from([("unknown", 0_u32)]);
    let products: AHashMap<_, _> = lines(body(&product_bytes))
        .filter_map(fields::<2>)
        .map(|[p, c]| {
            let code = *category_ids.entry(c).or_insert_with(|| {
                let code = names.len() as u32;
                names.push(c);
                code
            });
            (p, code)
        })
        .collect();
    let (mut campaign_names, mut channels) = (vec!["organic"], vec!["organic"]);
    let campaign_ids: AHashMap<_, _> = lines(body(&campaign_bytes))
        .filter_map(fields::<2>)
        .map(|[n, c]| {
            let code = campaign_names.len() as u32;
            campaign_names.push(n);
            channels.push(c);
            (n, code)
        })
        .collect();
    let link_maps = maps(&data.join("links"))?;
    let ids = identities(&link_maps);
    let events = maps(&data.join("events"))?;
    let clicks = maps(&data.join("clicks"))?;
    let event_chunks = chunks(&events);
    let hash = RandomState::new();
    let slot_count = if events.iter().map(|m| m.len()).sum::<usize>() > 3 * 1024 * 1024 * 1024 {
        1 << 26
    } else {
        1 << 25
    };
    let seen: Vec<_> = (0..slot_count / 64).map(|_| AtomicU64::new(0)).collect();
    let repeats: Vec<_> = (0..slot_count / 64).map(|_| AtomicU64::new(0)).collect();
    let slot = |id: &[u8]| event_slot(id, slot_count - 1);
    event_chunks.par_iter().for_each(|part| {
        for line in lines(part) {
            let mut id = memchr(b',', line).map_or(line, |i| &line[..i]);
            if id.len() >= 2 && id[0] == b'"' && id[id.len() - 1] == b'"' {
                id = &id[1..id.len() - 1];
            }
            if id.is_empty() {
                continue;
            }
            let Some(s) = slot(id) else { continue };
            let bit = 1 << (s % 64);
            if seen[s / 64].fetch_or(bit, Relaxed) & bit != 0 {
                repeats[s / 64].fetch_or(bit, Relaxed);
            }
        }
    });
    drop(seen);
    let partitions: Vec<_> = (0..SHARDS).map(|_| Mutex::new(Vec::new())).collect();
    let duplicate_rows: Vec<_> = (0..SHARDS).map(|_| Mutex::new(Vec::new())).collect();
    let row_inputs = event_chunks
        .into_iter()
        .map(|c| (false, c))
        .chain(chunks(&clicks).into_iter().map(|c| (true, c)))
        .collect::<Vec<_>>();
    row_inputs.into_par_iter().for_each(|(click, part)| {
        let mut buckets: Vec<_> = (0..SHARDS).map(|_| Vec::new()).collect();
        let mut duplicates: Vec<_> = (0..SHARDS).map(|_| Vec::new()).collect();
        for line in lines(part) {
            if click {
                if let Some([id, person, campaign, ts]) = fields::<4>(line) {
                    if id.is_empty() || person.is_empty() {
                        continue;
                    }
                    if let (Some(&code), Some(ts)) = (campaign_ids.get(campaign), timestamp(ts)) {
                        if ts >= start - WEEK && ts < end {
                            route(
                                Row {
                                    person,
                                    ts,
                                    code,
                                    revenue: 0,
                                    click: Some(id),
                                },
                                &ids,
                                &hash,
                                &mut buckets,
                            );
                        }
                    }
                }
            } else if let Some(f) = fields::<7>(line) {
                if f[0].is_empty() {
                    continue;
                }
                let parsed = event(f, start, end, &products);
                let unique = slot(f[0].as_bytes())
                    .is_some_and(|s| repeats[s / 64].load(Relaxed) & (1 << (s % 64)) == 0);
                if unique {
                    if let Some(row) = parsed.row {
                        route(row, &ids, &hash, &mut buckets);
                    }
                } else {
                    let shard = hash.hash_one(f[0]) as usize % SHARDS;
                    duplicates[shard].push((f[0], parsed));
                }
            }
        }
        for (dest, mut rows) in partitions.iter().zip(buckets) {
            if !rows.is_empty() {
                dest.lock().unwrap().append(&mut rows);
            }
        }
        for (dest, mut rows) in duplicate_rows.iter().zip(duplicates) {
            if !rows.is_empty() {
                dest.lock().unwrap().append(&mut rows);
            }
        }
    });
    duplicate_rows.into_par_iter().for_each(|rows| {
        let mut winners = AHashMap::new();
        for (id, event) in rows.into_inner().unwrap() {
            let winner = winners.entry(id).or_insert(event);
            if event.ingest < winner.ingest {
                *winner = event;
            }
        }
        let mut buckets: Vec<_> = (0..SHARDS).map(|_| Vec::new()).collect();
        for row in winners.into_values().filter_map(|e| e.row) {
            route(row, &ids, &hash, &mut buckets);
        }
        for (dest, mut rows) in partitions.iter().zip(buckets) {
            if !rows.is_empty() {
                dest.lock().unwrap().append(&mut rows);
            }
        }
    });
    let user_dir = out.join("user_daily");
    let campaign_dir = out.join("campaign_daily");
    fs::create_dir_all(&user_dir)?;
    fs::create_dir_all(&campaign_dir)?;
    let partials: Vec<_> = partitions
        .into_par_iter()
        .enumerate()
        .map(|(i, rows)| {
            aggregate(
                rows.into_inner().unwrap(),
                &names,
                campaign_names.len(),
                &user_dir.join(format!("part-{i}.parquet")),
            )
        })
        .collect::<Result<_, _>>()?;
    let mut totals = Campaigns::default();
    for partial in partials {
        for (key, row) in partial {
            let t = totals.entry(key).or_default();
            t.sessions += row.sessions;
            t.buying += row.buying;
            t.revenue += row.revenue;
            t.assists += row.assists;
        }
    }
    let rows: Vec<_> = totals.into_iter().collect();
    let batch = campaign_batch(&rows, &campaign_names, &channels);
    let mut output = writer(&campaign_dir.join("part-0.parquet"), &batch)?;
    if !rows.is_empty() {
        output.write(&batch)?;
    }
    output.close()?;
    Ok(())
}
