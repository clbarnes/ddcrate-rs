use anyhow as ah;
use anyhow::anyhow;
use chrono::format::Parsed;
use clap::Parser;
use once_cell_regex::{exports::regex::Captures, regex};
use std::fmt::Debug;
use std::fs;
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
    str::FromStr,
};

use chrono::{DateTime, Datelike, FixedOffset, NaiveDate, NaiveDateTime, NaiveTime, Utc};

use ddcrate::{
    default_levels, rank_players, Config, Level, PlayerId, PlayerRecord, ResultIngester,
    FINISH_DECAY,
};

/// Read a directory of directories of TSV files reporting tournament finishing places,
/// and print a TSV with columns rank, rating, player ID.
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Directory containing directories of TSV results.
    #[arg(short, long)]
    dir: PathBuf,
    #[arg(short, long)]
    sorted: bool,
    #[arg(short, long)]
    from: Option<String>,
    #[arg(short, long)]
    to: Option<String>,
    #[arg(short = 'C', long)]
    config: Option<PathBuf>,
    #[arg(short = 'S', long)]
    no_short: bool,
    #[arg(short = 'E', long)]
    no_medium: bool,
    #[arg(short = 'M', long)]
    no_major: bool,
    #[arg(short = 'C', long)]
    no_championship: bool,
    // todo: output format e.g. JSON
    // todo: full config?
    // #[arg(short, long)]
    // finish_decay: Option<f64>,
    // #[arg(short, long)]
    // age_decay: Option<f64>,
    // #[arg(short, long)]
    // record_length: Option<usize>,
    // #[arg(short, long)]
    // small_points: Option<f64>,
    // #[arg(short = 'm', long)]
    // medium_points: Option<f64>,
    // #[arg(short = 'M', long)]
    // major_points: Option<f64>,
    // #[arg(short, long)]
    // championship_points: Option<f64>,
}

fn print_record(pid_rank: (PlayerId, u64), records: &HashMap<PlayerId, PlayerRecord>) {
    println!(
        "{}\t{}\t{}",
        pid_rank.1, records[&pid_rank.0].rating, pid_rank.0
    );
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
    let offset = match sign {
        '-' => -seconds,
        _ => seconds,
    };

    parsed.set_offset(seconds).map_err(|_| "Invalid offset")?;
    let naive = parsed
        .to_naive_datetime_with_offset(0)
        .map_err(|_| "Invalid datetime")?;
    Ok(DateTime::from_utc(naive, Utc))
}

fn main() -> ah::Result<()> {
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

    let levels = default_levels().clone();
    let mut level_set: HashSet<_> = levels.keys().collect();

    if args.no_short {
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

    let tournaments = ingest.ingest()?;
    let (ranks, records) = rank_players(tournaments.as_slice(), year, &config);
    if args.sorted {
        let mut sorted_ranks: Vec<_> = ranks.into_iter().collect();
        sorted_ranks.sort_unstable_by_key(|(_, rank)| *rank);
        sorted_ranks
            .into_iter()
            .for_each(|pr| print_record(pr, &records));
    } else {
        ranks.into_iter().for_each(|pr| print_record(pr, &records));
    }

    Ok(())
}
