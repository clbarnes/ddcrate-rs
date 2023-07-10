use anyhow::{anyhow, Result};
use chrono::format::Parsed;
use clap::Parser;
use csv::ReaderBuilder;
use once_cell_regex::{exports::regex::Captures, regex};
use std::fmt::Debug;
use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Write};
use std::path::Path;
use std::{collections::HashMap, path::PathBuf, str::FromStr};

use chrono::{DateTime, Datelike, Utc};

use ddcrate::{rank_players, Config, Level, PlayerId, PlayerRecord, ResultIngester};

/// Read a directory of directories of TSV files reporting tournament finishing places,
/// and print a TSV with columns rank, rating, player ID.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory containing directories of TSV results.
    #[arg(short, long)]
    dir: PathBuf,
    /// Sort output by player rank
    #[arg(short, long)]
    sorted: bool,
    /// Only include results from this datetime, as RFC 3339.
    /// Elements can be dropped from the right,
    /// in which case the parser assumes it's the earliest matching datetime (in UTC).
    /// For example, valid dates include `2022-06-25T12:00:05+04:00`,
    /// and `2022` (which is interpreted as `2022-01-01T00:00:00+00:00`).
    #[arg(short, long)]
    from: Option<String>,
    /// Only include results from before this datetime, as RFC 3339.
    /// See --from docs for parsing details;
    /// although truncated datetimes are assumed to be the latest match.
    #[arg(short, long)]
    to: Option<String>,
    /// Path to TOML config file with algorithm constants.
    #[arg(short = 'C', long)]
    config: Option<PathBuf>,
    /// Ignore results from "small" tournaments.
    #[arg(short = 'S', long)]
    no_small: bool,
    /// Ignore results from "medium" tournaments.
    #[arg(short = 'E', long)]
    no_medium: bool,
    /// Ignore results from "major" tournaments.
    #[arg(short = 'M', long)]
    no_major: bool,
    /// Ignore results from "championship" tournaments.
    #[arg(short = 'C', long)]
    no_championship: bool,
    /// Skip column headers in output TSV.
    #[arg(short = 'H', long)]
    no_headers: bool,
    /// Path to player database; a TSV where the first column is player ID
    /// and the remainder is the player name.
    /// If not given, the player_name column will be omitted.
    #[arg(short, long)]
    players: Option<PathBuf>,
}

pub struct RecordWriter<W: Write> {
    writer: W,
    records: HashMap<PlayerId, PlayerRecord>,
    players: Option<HashMap<PlayerId, String>>,
}

impl<W: Write> RecordWriter<W> {
    pub fn write_headers(&mut self) -> io::Result<()> {
        write!(&mut self.writer, "rank\trating\tplayer_id")?;
        if self.players.is_some() {
            write!(&mut self.writer, "\tplayer_name")?;
        }
        write!(&mut self.writer, "\n")
    }

    pub fn write_record(&mut self, id: PlayerId, rank: u64) -> io::Result<()> {
        write!(
            &mut self.writer,
            "{}\t{}\t{}",
            rank, self.records[&id].rating, id
        )?;
        if let Some(ps) = &self.players {
            if let Some(name) = ps.get(&id) {
                write!(&mut self.writer, "\t{}", name)?;
            }
        }
        write!(&mut self.writer, "\n")
    }
}

fn parse_capture<T>(cap: &Captures, name: &str, default: T) -> T
where
    T: FromStr + Debug,
    <T as FromStr>::Err: Debug,
{
    cap.name(name)
        .map(|m| m.as_str().parse().unwrap())
        .unwrap_or(default)
}

const MONTH_DAYS: [i64; 12] = [
    31, // Jan
    28, // Feb
    31, // Mar
    30, // Apr
    31, // May
    30, // Jun
    31, // Jul
    31, // Aug
    30, // Sep
    31, // Oct
    30, // Nov
    31, // Dec
];

fn parse_player_db(p: &Path) -> Result<HashMap<PlayerId, String>> {
    let f = BufReader::new(File::open(p)?);
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'\t')
        .comment(Some(b'#'))
        .from_reader(f);

    let mut out = HashMap::default();
    for result in rdr.records() {
        let record = result?;
        let Some(id_str) = record.get(0) else {continue;};
        let Ok(player) = id_str.parse::<PlayerId>() else {continue;};
        let Some(name) = record.get(1) else {continue;};
        out.insert(player, name.to_owned());
    }
    Ok(out)
}

fn parse_datetime(s: &str, up: bool) -> Result<DateTime<Utc>, &'static str> {
    let re = regex!(
        r"(?x)
        (?P<year>\d\d\d\d)
        (-(?P<month>\d\d)
        (-(?P<day>\d\d)
        (T(?P<hour>\d\d)
        (:(?P<min>\d\d)
        (:(?P<sec>\d\d)
        ((?P<offset>[+-]\d\d:?\d\d)
    )?)?)?)?)?)?
    "
    );
    let Some(cap) = re.captures(s) else {return Err("Could not parse datetime")};

    let mut parsed = Parsed::new();

    let year = cap["year"].parse().unwrap();
    parsed.set_year(year).map_err(|_| "Invalid year")?;
    let month = parse_capture(&cap, "month", if up { 12 } else { 1 });
    parsed.set_month(month).map_err(|_| "Invalid month")?;
    if !(1..=12).contains(&month) {
        return Err("Invalid month");
    }
    let n_days = if up {
        let mut n_days = MONTH_DAYS[(month - 1) as usize];
        if month == 2 && year % 4 == 0 {
            n_days += 1;
        }
        n_days
    } else {
        1
    };
    let day = parse_capture(&cap, "month", n_days);
    parsed.set_day(day).map_err(|_| "Invalid day")?;

    let hour = parse_capture(&cap, "hour", if up { 23 } else { 0 });
    parsed.set_hour(hour).map_err(|_| "Invalid hour")?;
    let min = parse_capture(&cap, "min", if up { 59 } else { 0 });
    parsed.set_minute(min).map_err(|_| "Invalid minute")?;
    let sec = parse_capture(&cap, "sec", if up { 59 } else { 0 });
    parsed.set_second(sec).map_err(|_| "Invalid second")?;

    let offset_str = cap
        .name("offset")
        .map(|m| m.as_str().replace(':', ""))
        .unwrap_or("+0000".to_owned());

    let mut chars = offset_str.chars();
    let sign = chars.next().unwrap();
    let mut seconds: i64 = 0;
    let mut buf: [u8; 4] = [0; 4];
    seconds += chars
        .next()
        .unwrap()
        .encode_utf8(&mut buf)
        .parse::<i64>()
        .unwrap()
        * 60
        * 60
        * 10;
    seconds += chars
        .next()
        .unwrap()
        .encode_utf8(&mut buf)
        .parse::<i64>()
        .unwrap()
        * 60
        * 60;
    seconds += chars
        .next()
        .unwrap()
        .encode_utf8(&mut buf)
        .parse::<i64>()
        .unwrap()
        * 60
        * 10;
    seconds += chars
        .next()
        .unwrap()
        .encode_utf8(&mut buf)
        .parse::<i64>()
        .unwrap()
        * 60;
    let _offset = match sign {
        '-' => -seconds,
        _ => seconds,
    };

    parsed.set_offset(seconds).map_err(|_| "Invalid offset")?;
    let naive = parsed
        .to_naive_datetime_with_offset(0)
        .map_err(|_| "Invalid datetime")?;
    Ok(DateTime::from_utc(naive, Utc))
}

fn main() -> Result<()> {
    let args = Args::parse();

    let config: Config = if let Some(p) = args.config {
        let contents = fs::read_to_string(p)?;
        toml::from_str(&contents)?
    } else {
        Config::default()
    };

    let mut ingest = ResultIngester::new(args.dir);
    let mut year = Utc::now().year();
    if let Some(from_str) = args.from {
        ingest = ingest.from(parse_datetime(&from_str, false).map_err(|e| anyhow!(e))?);
    }
    if let Some(to_str) = args.to {
        let dt = parse_datetime(&to_str, true).map_err(|e| anyhow!(e))?;
        ingest = ingest.until(dt);
        year = dt.year();
    }

    let mut level_set = Level::all();

    if args.no_small {
        level_set.remove(&Level::Small);
    }
    if args.no_medium {
        level_set.remove(&Level::Medium);
    }
    if args.no_major {
        level_set.remove(&Level::Major);
    }
    if args.no_championship {
        level_set.remove(&Level::Championship);
    }
    if level_set.is_empty() {
        return Ok(());
    }

    ingest = ingest.levels(level_set);

    let players = args.players.map(|p| parse_player_db(&p)).transpose()?;

    let tournaments = ingest.ingest()?;
    let (ranks, records) = rank_players(tournaments.as_slice(), year, &config);
    let mut writer = RecordWriter {
        writer: BufWriter::new(io::stdout()),
        records,
        players,
    };
    if !args.no_headers {
        writer.write_headers()?;
    }
    if args.sorted {
        let mut sorted_ranks: Vec<_> = ranks.into_iter().collect();
        sorted_ranks.sort_unstable_by_key(|(pid, rank)| (*rank, *pid));
        sorted_ranks
            .into_iter()
            .for_each(|(id, rank)| writer.write_record(id, rank).unwrap());
    } else {
        ranks
            .into_iter()
            .for_each(|(id, rank)| writer.write_record(id, rank).unwrap());
    }
    Ok(())
}
