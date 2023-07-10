use chrono::{Datelike, NaiveDate, TimeZone};
use csv::ReaderBuilder;
use log::debug;
use once_cell::sync::OnceCell;
use once_cell_regex::regex;
use serde::Deserialize;
use std::collections::HashSet;
use std::io::{BufReader, Read};
use std::{
    cmp::Reverse,
    collections::{BinaryHeap, HashMap},
    fs::File,
    io,
    path::PathBuf,
};
use thiserror::Error;
use walkdir::WalkDir;

pub use chrono::{DateTime, Utc};
use ordered_float::NotNan;

pub type PlayerId = u64;

pub const FINISH_DECAY: f64 = 1.1;
pub const AGE_DECAY: f64 = 1.1;
pub const RECORD_LENGTH: usize = 10;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct Team {
    early: PlayerId,
    late: PlayerId,
}

impl Team {
    pub fn new(player1: PlayerId, player2: PlayerId) -> Result<Self, RepeatedPlayer> {
        match player1.cmp(&player2) {
            std::cmp::Ordering::Less => Ok(Self::new_unchecked(player1, player2)),
            std::cmp::Ordering::Equal => Err(RepeatedPlayer(player1)),
            std::cmp::Ordering::Greater => Ok(Self::new_unchecked(player2, player1)),
        }
    }

    pub fn new_unchecked(early: PlayerId, late: PlayerId) -> Self {
        Self { early, late }
    }

    pub fn players(&self) -> [&PlayerId; 2] {
        [&self.early, &self.late]
    }
}

#[derive(Debug, Copy, Clone, Hash, Eq, PartialEq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    Small,
    Medium,
    Major,
    Championship,
}

impl Level {
    pub fn point_base(&self) -> f64 {
        match self {
            Level::Small => 50.0,
            Level::Medium => 125.0,
            Level::Major => 200.0,
            Level::Championship => 250.0,
        }
    }

    pub fn directory_name(&self) -> &'static str {
        match self {
            Level::Small => "small",
            Level::Medium => "medium",
            Level::Major => "major",
            Level::Championship => "championship",
        }
    }

    pub fn all() -> HashSet<Self> {
        let mut out = HashSet::with_capacity(4);
        out.insert(Self::Small);
        out.insert(Self::Medium);
        out.insert(Self::Major);
        out.insert(Self::Championship);
        out
    }
}

#[derive(Debug, Clone)]
pub struct Tournament {
    /// Finishing position and team
    results: Vec<(u64, Team)>,
    datetime: DateTime<Utc>,
    level: Level,
}

#[derive(Debug, Error)]
#[error("Repeated player: {0}")]
pub struct RepeatedPlayer(PlayerId);

#[derive(Debug, Error)]
#[error("Ranks are inconsistent")]
pub struct InconsistentRanks();

#[derive(Debug, Error)]
pub enum InvalidTournament {
    #[error(transparent)]
    RepeatedPlayer(#[from] RepeatedPlayer),
    #[error(transparent)]
    InconsistentRanks(#[from] InconsistentRanks),
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    finish_decay: f64,
    age_decay: f64,
    record_length: usize,
    levels: HashMap<Level, f64>,
}

impl Config {
    pub fn new(
        finish_decay: f64,
        age_decay: f64,
        record_length: usize,
        levels: &HashMap<Level, f64>,
    ) -> Self {
        let default_levels = default_levels();
        let mut lvls = HashMap::with_capacity(default_levels.len());
        for (lvl, pb) in default_levels.iter() {
            let val = *levels.get(lvl).unwrap_or(pb);
            lvls.insert(*lvl, val);
        }
        Self {
            finish_decay,
            age_decay,
            record_length,
            levels: lvls,
        }
    }

    pub fn finish_decay(mut self, finish_decay: f64) -> Self {
        self.finish_decay = finish_decay;
        self
    }

    pub fn age_decay(mut self, age_decay: f64) -> Self {
        self.age_decay = age_decay;
        self
    }

    pub fn record_length(mut self, record_length: usize) -> Self {
        self.record_length = record_length;
        self
    }

    pub fn level(mut self, level: Level, point_base: f64) -> Self {
        self.levels.insert(level, point_base);
        self
    }
}

const LEVEL_PAIRS: [(Level, f64); 4] = [
    (Level::Small, 50.0),
    (Level::Medium, 125.0),
    (Level::Major, 200.0),
    (Level::Championship, 250.0),
];

fn level_init() -> HashMap<Level, f64> {
    LEVEL_PAIRS.into_iter().collect()
}

static LEVELS: OnceCell<HashMap<Level, f64>> = OnceCell::new();

pub fn default_levels() -> &'static HashMap<Level, f64> {
    LEVELS.get_or_init(level_init)
}

impl Default for Config {
    fn default() -> Self {
        Self {
            finish_decay: FINISH_DECAY,
            age_decay: AGE_DECAY,
            record_length: RECORD_LENGTH,
            levels: default_levels().clone(),
        }
    }
}

impl Tournament {
    pub fn new(
        mut results: Vec<(u64, Team)>,
        datetime: DateTime<Utc>,
        level: Level,
    ) -> Result<Self, InvalidTournament> {
        let mut prev_place: u64 = 0;
        let mut increment: u64 = 1;
        results.sort_unstable_by_key(|p| p.0);
        let mut players = HashSet::with_capacity(results.len() * 4);
        for (place, team) in results.iter() {
            for player in team.players() {
                if players.contains(player) {
                    return Err(RepeatedPlayer(*player).into());
                }
                players.insert(*player);
            }
            if &prev_place == place {
                increment += 1;
            } else if place != &(prev_place + increment) {
                return Err(InconsistentRanks().into());
            } else {
                increment = 1;
            }
            prev_place += increment;
        }
        Ok(Self::new_unchecked(results, datetime, level))
    }

    pub fn new_unchecked(results: Vec<(u64, Team)>, datetime: DateTime<Utc>, level: Level) -> Self {
        Self {
            results,
            datetime,
            level,
        }
    }

    pub fn points(
        &self,
        current_season: i32,
        initial_ranks: &HashMap<PlayerId, u64>,
        config: &Config,
    ) -> HashMap<PlayerId, NotNan<f64>> {
        if self.results.is_empty() {
            return self
                .results
                .iter()
                .flat_map(|(_, t)| t.players())
                .map(|p| (*p, NotNan::new(0.0).unwrap()))
                .collect();
        }
        let mut out = HashMap::with_capacity(self.results.len() * 2);
        let mut bonus: f64 = 0.0;
        let age = (current_season - self.datetime.year()) as f64;
        let mut bonus_update: f64 = 0.0;
        let mut prev_place = self.results.last().unwrap().0 + 1;
        let point_base = config.levels[&self.level];
        for (place, team) in self.results.iter().rev() {
            for player in team.players() {
                let mut points = point_base * (1.0 / FINISH_DECAY.powi(*place as i32));
                points *= 1.0 / AGE_DECAY.powf(age);
                points += bonus;
                out.insert(*player, NotNan::new(points / 2.0).unwrap());
                bonus_update += bonus_points(*initial_ranks.get(player).unwrap_or(&201));
            }
            if place != &prev_place {
                bonus += bonus_update;
                bonus_update = 0.0;
                prev_place = *place;
            }
        }
        out
    }
}

fn bonus_points(rank: u64) -> f64 {
    let bpoints = vec![
        (5, 10.0),
        (10, 7.5),
        (20, 5.0),
        (50, 2.5),
        (100, 1.0),
        (200, 0.5),
    ];

    for (max_rank, points) in bpoints {
        if rank <= max_rank {
            return points;
        }
    }
    0.0
}

#[derive(Debug, Clone)]
pub struct PlayerRecord {
    pub id: PlayerId,
    points: BinaryHeap<Reverse<NotNan<f64>>>,
    pub rating: NotNan<f64>,
}

impl PlayerRecord {
    pub fn new(id: PlayerId, record_length: usize) -> Self {
        Self {
            id,
            points: BinaryHeap::with_capacity(record_length + 1),
            rating: NotNan::new(0.0).unwrap(),
        }
    }

    pub fn new_with_points(id: PlayerId, record_length: usize, points: &[f64]) -> Self {
        let mut player = Self::new(id, record_length);
        for p in points.iter() {
            player.add_result(NotNan::new(*p).unwrap());
        }
        player
    }

    pub fn add_result(&mut self, points: NotNan<f64>) -> (bool, NotNan<f64>) {
        let p = Reverse(points);
        if self.points.len() < RECORD_LENGTH {
            self.rating += points;
            self.points.push(p);
            return (points != 0.0, self.rating);
        }

        self.points.push(p);
        let removed = self.points.pop().unwrap().0;
        if removed == points {
            (false, self.rating)
        } else {
            self.rating = self.rating - removed + points;
            (true, self.rating)
        }
    }
}

fn records_to_update_ranks(
    records: &HashMap<PlayerId, PlayerRecord>,
    into: &mut HashMap<PlayerId, u64>,
) {
    into.clear();
    let mut pid_scores: Vec<_> = records
        .iter()
        .map(|(pid, rec)| (*pid, rec.rating))
        .collect();
    pid_scores.sort_unstable_by_key(|(_, rat)| *rat);
    let mut prev_rank = 0;
    let mut rank_incr = 1;
    let mut prev_score = NotNan::new(-1.0).unwrap();

    for (pid, score) in pid_scores {
        if score == prev_score {
            rank_incr += 1;
        } else {
            prev_rank += rank_incr;
            rank_incr = 1;

            prev_score = score;
        }

        into.insert(pid, prev_rank);
    }
}

/// Tournaments must be pre-sorted.
pub fn rank_players(
    tournaments: &[Tournament],
    current_season: i32,
    config: &Config,
) -> (HashMap<PlayerId, u64>, HashMap<PlayerId, PlayerRecord>) {
    let mut prev_dt = DateTime::<Utc>::MIN_UTC;
    let mut ranks: HashMap<PlayerId, u64> = Default::default();
    let mut records: HashMap<PlayerId, PlayerRecord> = Default::default();
    let mut needs_updating = true;
    for t in tournaments.iter() {
        for (pid, pts) in t.points(current_season, &ranks, config).iter() {
            let record = records
                .entry(*pid)
                .or_insert_with(|| PlayerRecord::new(*pid, config.record_length));
            record.add_result(*pts);
        }
        match prev_dt.cmp(&t.datetime) {
            std::cmp::Ordering::Less => {
                records_to_update_ranks(&records, &mut ranks);
                prev_dt = t.datetime;
                needs_updating = false;
            }
            std::cmp::Ordering::Equal => {
                needs_updating = true;
            }
            std::cmp::Ordering::Greater => panic!("Tournaments were not ordered"),
        }
    }
    if needs_updating {
        records_to_update_ranks(&records, &mut ranks);
    }
    (ranks, records)
}

#[derive(Debug, Error)]
pub enum ResultReadError {
    #[error(transparent)]
    InvalidTournament(#[from] InvalidTournament),
    #[error(transparent)]
    Io(#[from] io::Error),
}

pub struct ResultIngester {
    root: PathBuf,
    levels: HashSet<Level>,
    from: DateTime<Utc>,
    until: DateTime<Utc>,
}

impl ResultIngester {
    pub fn new<P: Into<PathBuf>>(root: P) -> Self {
        Self {
            root: root.into(),
            levels: Level::all(),
            from: DateTime::<Utc>::MIN_UTC,
            until: DateTime::<Utc>::MAX_UTC,
        }
    }

    pub fn levels(mut self, levels: HashSet<Level>) -> Self {
        self.levels = levels;
        self
    }

    pub fn from(mut self, from: DateTime<Utc>) -> Self {
        self.from = from;
        self
    }

    pub fn until(mut self, until: DateTime<Utc>) -> Self {
        self.until = until;
        self
    }

    pub fn ingest_level(&self, level: Level) -> Result<Vec<Tournament>, ResultReadError> {
        let mut out = Vec::default();
        let dname = level.directory_name();
        let mut d = self.root.clone();
        d.push(dname);
        if !d.is_dir() {
            return Ok(out);
        }
        let tsv_re = regex!(r"(?P<date>\d\d\d\d-\d\d-\d\d).*\.tsv");
        for entry in WalkDir::new(d).follow_links(true) {
            // todo: parallelise reading
            let e = entry.map_err(|e| {
                e.into_io_error().unwrap_or(io::Error::new(
                    io::ErrorKind::Other,
                    "Error reading directories",
                ))
            })?;
            if !e.file_type().is_file() {
                continue;
            }
            let fname = e.file_name().to_str().expect("Non UTF-8 file name");
            let Some(cap) = tsv_re.captures(fname) else {continue};
            let date_str = &cap["date"];
            let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d").unwrap();
            let dt = Utc
                .with_ymd_and_hms(date.year(), date.month(), date.day(), 0, 0, 0)
                .unwrap();

            if dt < self.from || dt > self.until {
                continue;
            }

            let rd = BufReader::new(File::open(e.path())?);
            let ranks = parse_ranks(rd)?;
            out.push(Tournament::new(ranks, dt, level)?);
        }
        Ok(out)
    }

    pub fn ingest(&self) -> Result<Vec<Tournament>, ResultReadError> {
        let mut out = Vec::default();
        for level in self.levels.iter() {
            let mut v = self.ingest_level(*level)?;
            out.append(&mut v);
        }
        Ok(out)
    }
}

pub fn parse_ranks<R: Read>(r: R) -> Result<Vec<(u64, Team)>, ResultReadError> {
    let mut ranks = Vec::default();
    let mut rdr = ReaderBuilder::new()
        .delimiter(b'\t')
        .comment(Some(b'#'))
        .from_reader(r);

    for result in rdr.records() {
        let record =
            result.map_err(|_| io::Error::new(io::ErrorKind::Other, "Could not parse TSV"))?;
        let Some(rank_str) = record.get(0) else {continue};
        let Ok(rank) = rank_str.parse::<u64>() else {
            debug!("Could not parse '{}' as rank, skipping", rank_str);
            continue;
        };
        let Some(p1_str) = record.get(1) else {
            debug!("No player 1 field, skipping");
            continue;
        };
        let Ok(p1) = p1_str.parse::<PlayerId>() else {
            debug!("Could not parse '{}' as player ID, skipping", p1_str);
            continue;
        };
        let Some(p2_str) = record.get(2) else {
            debug!("No player 2 field, skipping");
            continue;
        };
        let Ok(p2) = p2_str.parse::<PlayerId>() else {
            debug!("Could not parse '{}' as player ID, skipping", p2_str);
            continue;
        };
        ranks.push((
            rank,
            Team::new(p1, p2).map_err(|e| ResultReadError::from(InvalidTournament::from(e)))?,
        ));
    }
    Ok(ranks)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    fn data_dir() -> PathBuf {
        let mut d = PathBuf::from(PathBuf::from(env!("CARGO_MANIFEST_DIR")).parent().unwrap());
        d.push("example_data");
        d
    }

    #[test]
    fn config_deser() {
        let mut path = data_dir();
        path.push("default_config.toml");
        let contents = fs::read_to_string(path).expect("Could not read");
        let config: Config = toml::from_str(&contents).expect("Could not parse");
        assert_eq!(config.finish_decay, 1.1);
        assert_eq!(config.age_decay, 1.1);
        assert_eq!(config.record_length, 10);
        assert_eq!(config.levels[&Level::Small], 50.0);
        assert_eq!(config.levels[&Level::Medium], 125.0);
        assert_eq!(config.levels[&Level::Major], 200.0);
        assert_eq!(config.levels[&Level::Championship], 250.0);
    }
}
