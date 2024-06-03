use loginus::journald::{Entry, JournalExportRead, JournalExportReadError, RefEntry};
use rand::Rng;
use sha2::Digest;
use std::{
    fs::OpenOptions,
    io::{self, Write},
    path::PathBuf,
};

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Merge {
        #[arg(short, long)]
        out: PathBuf,
        srcs: Vec<PathBuf>,
    },
    Sample {
        #[arg(short, long)]
        sample_rate: f64,
        #[arg(short, long)]
        out: PathBuf,
        src: PathBuf,
    },
    Split {
        #[arg(short, long)]
        out_dir: PathBuf,
        src: PathBuf,
    },
    Count {
        src: PathBuf,
    },
    ShowEntry {
        src: PathBuf,
        n: usize,
    },
}

fn main() -> io::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::Merge { out, srcs } => merge_journals(out, srcs)?,
        Command::Sample {
            sample_rate,
            out,
            src,
        } => sample_journal(out, sample_rate, src)?,
        Command::Split { out_dir, src } => split(out_dir, src)?,
        Command::Count { src } => {
            let c = count(src)?;
            println!("{}", c);
        }
        Command::ShowEntry { src, n } => show_entry(src, n)?,
    }

    Ok(())
}

fn merge_journals(out: PathBuf, srcs: Vec<PathBuf>) -> std::io::Result<()> {
    let mut jreaders = vec![];
    srcs.iter().try_for_each(|p| {
        jreaders.push(JournalExportRead::new(
            OpenOptions::new().read(true).open(p)?,
        ));
        Ok::<_, std::io::Error>(())
    })?;
    let mut outfile = OpenOptions::new().create(true).write(true).open(out)?;

    let mut counts = vec![];
    for idx in 0..jreaders.len() {
        if let Err(JournalExportReadError::Eof) = jreaders[idx].parse_next() {
            jreaders.remove(idx);
        } else {
            counts.push(0);
        }
    }
    println!("jreaders.len(): {}", jreaders.len());
    while !jreaders.is_empty() {
        let mut min_idx = 0;
        let mut min_val = u64::MAX - 1;
        for (idx, _) in jreaders.iter().enumerate() {
            let val = get_time_stamp(jreaders[idx].get_entry());
            if val < min_val {
                min_val = val;
                min_idx = idx;
                counts[idx] += 1;
            }
        }
        outfile.write_all(jreaders[min_idx].get_entry().as_bytes())?;

        match jreaders[min_idx].parse_next() {
            Err(JournalExportReadError::Eof) => {
                jreaders.remove(min_idx);
                println!("count at {}: {}", min_idx, counts[min_idx]);
                counts.remove(min_idx);
            }
            Err(JournalExportReadError::IoError(e)) => return Err(e),
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
            Ok(_) => (),
        }
    }
    outfile.flush()?;
    Ok(())
}

fn sample_journal(dst: PathBuf, sample_rate: f64, src: PathBuf) -> io::Result<()> {
    let mut jreader = JournalExportRead::new(OpenOptions::new().read(true).open(src)?);
    let mut outfile = OpenOptions::new().create(true).write(true).open(dst)?;

    let mut rng = rand::thread_rng();
    loop {
        match jreader.parse_next() {
            Ok(_) => (),
            Err(JournalExportReadError::Eof) => return Ok(()),
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
        }

        if rng.gen_bool(sample_rate) {
            outfile.write_all(jreader.get_entry().as_bytes())?;
        }
    }
}

fn split(out_dir: PathBuf, src: PathBuf) -> io::Result<()> {
    let mut jreader = JournalExportRead::new(OpenOptions::new().read(true).open(src)?);

    loop {
        match jreader.parse_next() {
            Ok(_) => (),
            Err(JournalExportReadError::Eof) => return Ok(()),
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
        }

        let e = jreader.get_entry();
        let digest: [u8; 32] = sha2::Sha256::digest(e.as_bytes()).into();
        let digest = digest.iter().fold(String::new(), |mut s, b| {
            s.push_str(&format!("{:02x}", b));
            s
        });
        let target = out_dir.join(&digest);
        std::fs::write(target, e.as_bytes())?;
    }
}

fn get_time_stamp(entry: RefEntry<'_>) -> u64 {
    for (name, content, _) in entry.iter() {
        if name == b"__REALTIME_TIMESTAMP" {
            return String::from_utf8_lossy(content)
                .parse::<u64>()
                .unwrap_or(u64::MAX);
        }
    }
    u64::MAX
}

fn count(src: PathBuf) -> io::Result<usize> {
    let mut jreader = JournalExportRead::new(OpenOptions::new().read(true).open(src)?);

    let mut count = 0;
    loop {
        match jreader.parse_next() {
            Ok(_) => (),
            Err(JournalExportReadError::Eof) => return Ok(count),
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
        }

        count += 1;
    }
}

fn show_entry(src: PathBuf, n: usize) -> io::Result<()> {
    let mut jreader = JournalExportRead::new(OpenOptions::new().read(true).open(src)?);

    let mut count = 0;
    loop {
        match jreader.parse_next() {
            Ok(_) => (),
            Err(JournalExportReadError::Eof) => return Ok(()),
            Err(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
        }

        if count == n {
            for (name, content, _) in jreader.get_entry().iter() {
                let name = String::from_utf8_lossy(name);
                let content = String::from_utf8_lossy(content);
                println!("{}={}", name, content);
            }
            return Ok(());
        }
        count += 1;
    }
}
