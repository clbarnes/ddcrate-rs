use chrono::{Datelike, NaiveDate, TimeZone};
use csv::ReaderBuilder;
use regex::Regex;
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

#[derive(Debug, Clone, Copy)]
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

#[derive(Debug, Copy, Clone)]
pub enum TournamentType {
    Small,
    Medium,
    Major,
    Championship,
}

impl TournamentType {
    pub fn point_base(&self) -> f64 {
        match self {
            TournamentType::Small => 50.0,
            TournamentType::Medium => 125.0,
            TournamentType::Major => 200.0,
            TournamentType::Championship => 250.0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Tournament {
    /// Finishing position and team
    results: Vec<(u64, Team)>,
    datetime: DateTime<Utc>,
    ttype: TournamentType,
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

impl Tournament {
    pub fn new(
        mut results: Vec<(u64, Team)>,
        datetime: DateTime<Utc>,
        ttype: TournamentType,
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
        Ok(Self::new_unchecked(results, datetime, ttype))
    }

    pub fn new_unchecked(
        results: Vec<(u64, Team)>,
        datetime: DateTime<Utc>,
        ttype: TournamentType,
    ) -> Self {
        Self {
            results,
            datetime,
            ttype,
        }
    }

    pub fn points(
        &self,
        current_season: i32,
        initial_ranks: &HashMap<PlayerId, u64>,
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
        for (place, team) in self.results.iter().rev() {
            for player in team.players() {
                let mut points = self.ttype.point_base() * (1.0 / FINISH_DECAY.powi(*place as i32));
                points = points * (1.0 / AGE_DECAY.powf(age));
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
    return 0.0;
}

#[derive(Debug, Clone)]
pub struct PlayerRecord {
    pub id: PlayerId,
    points: BinaryHeap<Reverse<NotNan<f64>>>,
    pub rating: NotNan<f64>,
}

impl PlayerRecord {
    pub fn new(id: PlayerId) -> Self {
        Self {
            id,
            points: BinaryHeap::with_capacity(RECORD_LENGTH + 1),
            rating: NotNan::new(0.0).unwrap(),
        }
    }

    pub fn new_with_points(id: PlayerId, points: &[f64]) -> Self {
        let mut player = Self::new(id);
        for p in points.iter() {
            player.add_result(NotNan::new(*p).unwrap());
        }
        player
    }

    pub fn add_result(&mut self, points: NotNan<f64>) -> (bool, NotNan<f64>) {
        let p = Reverse(points);
        if self.points.len() < RECORD_LENGTH {
            self.rating = self.rating + points;
            self.points.push(p);
            return (points != 0.0, self.rating);
        }

        self.points.push(p);
        let removed = self.points.pop().unwrap().0;
        if removed == points {
            return (false, self.rating);
        } else {
            self.rating = self.rating - removed + points;
            return (true, self.rating);
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
) -> (HashMap<PlayerId, u64>, HashMap<PlayerId, PlayerRecord>) {
    let mut prev_dt = DateTime::<Utc>::MIN_UTC;
    let mut ranks: HashMap<PlayerId, u64> = Default::default();
    let mut records: HashMap<PlayerId, PlayerRecord> = Default::default();
    let mut needs_updating = true;
    for t in tournaments.iter() {
        for (pid, pts) in t.points(current_season, &ranks).iter() {
            let record = records
                .entry(*pid)
                .or_insert_with(|| PlayerRecord::new(*pid));
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

/// Point to a directory which has any/ all directories `small/`, `medium/`, `major/`, `championship/`.
/// These may contain an arbitrary directory tree containing TSV files whose names start with an ISO-8601 date, and end with `.tsv`.
/// For example: `small/uk/cambs/2023-07-09_my-local-tournament.tsv`.
/// These must have 3 columns, describing the teams' finishing positions and player IDs, like this
/// (note also the handling of ties):
///
/// ```tsv
/// 1	235476	529052
/// 2	23342	4235211978
/// 2	234871	1387235
/// 4	5690845	5638906
/// ```
pub fn parse_result_dir<P: Into<PathBuf>>(
    dpath: P,
    sort: bool,
) -> Result<Vec<Tournament>, ResultReadError> {
    let mut out = Vec::default();
    let p: PathBuf = dpath.into();

    if !p.is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "containing directory does not exist",
        )
        .into());
    }

    let tsv_re = Regex::new(r"(?P<date>\d\d\d\d-\d\d-\d\d).*\.tsv").unwrap();

    for (dname, ttype) in vec![
        ("small", TournamentType::Small),
        ("medium", TournamentType::Medium),
        ("major", TournamentType::Major),
        ("championship", TournamentType::Championship),
    ] {
        let mut d = p.clone();
        d.push(dname);
        if !d.is_dir() {
            continue;
        }
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

            let rd = BufReader::new(File::open(e.path())?);
            let ranks = parse_ranks(rd)?;
            out.push(Tournament::new(ranks, dt, ttype)?);
        }
    }
    if sort {
        out.sort_unstable_by_key(|t| t.datetime);
    }
    Ok(out)
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
        let rank: u64 = rank_str.parse().unwrap();
        let Some(p1_str) = record.get(1) else {continue};
        let p1: PlayerId = p1_str.parse().unwrap();
        let Some(p2_str) = record.get(2) else {continue};
        let p2: PlayerId = p2_str.parse().unwrap();
        ranks.push((
            rank,
            Team::new(p1, p2).map_err(|e| ResultReadError::from(InvalidTournament::from(e)))?,
        ));
    }
    Ok(ranks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {}
}
