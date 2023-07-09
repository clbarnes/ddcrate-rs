use anyhow::Result;
use clap::Parser;
use std::{collections::HashMap, path::PathBuf};

use chrono::{Datelike, Utc};

use ddcrate::{parse_result_dir, rank_players, PlayerId, PlayerRecord};

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
    // todo: output format e.g. JSON
    // todo: start/ end year
}

fn print_record(pid_rank: (PlayerId, u64), records: &HashMap<PlayerId, PlayerRecord>) {
    println!(
        "{}\t{}\t{}",
        pid_rank.1, records[&pid_rank.0].rating, pid_rank.0
    );
}

fn main() -> Result<()> {
    let args = Args::parse();

    let tournaments = parse_result_dir(args.dir, true)?;
    let year = Utc::now().year();
    let (ranks, records) = rank_players(tournaments.as_slice(), year);
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
