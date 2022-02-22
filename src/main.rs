use clap::Parser;
use indicatif::{FormattedDuration, HumanBytes, ProgressBar, ProgressStyle};
use rayon::iter::{Either, IntoParallelIterator, ParallelIterator};
use rayon::Scope;
use std::collections::HashMap;
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};

#[derive(Debug, Parser)]
#[clap(name = "fcp", about = "Multi-threaded copy...in rust!")]
struct Opt {
    /// Source directory
    source: String,

    /// Destination directory
    dest: String,
}

fn main() {
    let args = Opt::parse();

    let source = PathBuf::from(&args.source)
        .canonicalize()
        .unwrap_or_default()
        .as_os_str()
        .to_string_lossy()
        .into_owned();
    let dest = PathBuf::from(&args.dest)
        .canonicalize()
        .unwrap_or_default()
        .as_os_str()
        .to_string_lossy()
        .into_owned();
    if source == dest {
        println!("Source and destination are the same.");
        return;
    }

    copy(&args.source, &args.dest);
}

fn copy<U: AsRef<Path>, V: AsRef<Path>>(from: &U, to: &V) {
    let start = std::time::Instant::now();
    let source = PathBuf::from(from.as_ref());
    let dest = PathBuf::from(to.as_ref());

    let (tx, rx) = mpsc::channel();
    rayon::scope(|s| scan(&source, tx, s));
    let received: (Vec<_>, Vec<_>) = rx.into_iter().unzip();
    let (file_set, sizes) = received;
    let file_count = file_set.len();
    let total_size = sizes.into_iter().sum();

    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {bytes}/{total_bytes} ({eta})",
            )
            .progress_chars("#>-")
            // .on_finish(finish)
    );

    let (tx, rx) = mpsc::channel();
    let sender = tx.clone();
    let output = process_files(&pb, &source, &dest, file_set, sender).1;
    let err_out: (Vec<_>, Vec<_>) = output.into_iter().unzip();
    let (err_set, _) = err_out;
    let perm_failed = process_files(&pb, &source, &dest, err_set, tx).1;
    let sizes = rx.iter().collect::<Vec<_>>();
    let copied_data = sizes.iter().sum();

    pb.finish_at_current_pos();

    if !perm_failed.is_empty() {
        perm_failed
        .into_iter()
        .map(|f| f.1)
        .for_each(|f| println!("{}", f));
    println!(
        "Copied {} files of {}, {} of {} in {}",
        sizes.len(),
        file_count,
        HumanBytes(copied_data),
        HumanBytes(total_size),
        FormattedDuration(start.elapsed())
    );
    } else {
        println!(
            "Copied {} files, {} in {}",
            file_count,
            HumanBytes(total_size),
            FormattedDuration(start.elapsed())
        );
    }
}

type DirEntryResult = Result<DirEntry, std::io::Error>;

macro_rules! distribute_output {
    ($dothis:expr, $file:expr, $msg:expr, $err:ident) => {
        match $dothis {
            Ok(ref ok) => ok,
            Err(ref $err) => {
                let message = $msg;
                return Either::Right(($file, message));
            }
        }
    };
}

fn process_files<U: AsRef<Path> + Sync, V: AsRef<Path> + Sync>(
    pb: &ProgressBar,
    source: &U,
    dest: &V,
    files: Vec<DirEntryResult>,
    tx: Sender<u64>,
) -> (
    Vec<DirEntryResult>, Vec<(DirEntryResult, String)>,
) {
    // Won't ever get used, but it makes the compiler happy
    let default = PathBuf::from(".");

    files
        .into_par_iter()
        .map_with(tx, |s, x| (s.clone(), x))
        .partition_map(|f: (Sender<u64>, DirEntryResult)| {
            let (tx, file) = f;
            macro_rules! handleit { ($dothis:expr, $err:ident, $msg:expr) => {distribute_output!($dothis, file, $msg, $err) } }
            let entry = handleit!(file, err, format!("Error: {:?}", err.kind()));
            macro_rules! ioerr {
                ($step:expr, $err:expr) => {
                    format!("{}: {:?} for file {}", $step, $err, entry.path().as_os_str().to_string_lossy().into_owned())
                };
            }
            
            let spath = entry.path();
            pb.set_message(format!("{}", spath.display()));
            let strip = spath.strip_prefix(&source);
            let stem = handleit!(strip, err, ioerr!("Strip Prefix Error", err));
            let dpath = dest.as_ref().join(&stem);
            let size = handleit!(entry.metadata(), err, ioerr!("Metadata Fetch Error {}", err.kind())).len();
            handleit!(fs::create_dir_all(&dpath.parent().unwrap_or_else(|| default.as_path())), err, ioerr!("Directory Create Error", err.kind()));
            if dpath.exists() {
                let dmd = dpath.metadata();
                let d_metadata = handleit!(dmd, err, ioerr!("Metadata Fetch Error", err.kind()));
                let mut permissions = d_metadata.permissions();
                if permissions.readonly() {
                    permissions.set_readonly(false);
                    handleit!(fs::set_permissions(&dpath, permissions), err, ioerr!("Error unsetting readonly", err.kind()));
                }
            }
            handleit!(fs::copy(&spath, &dpath), err, ioerr!("Error while copying", err.kind()));
            tx.send(size).unwrap();
            pb.inc(size);
            Either::Left(file)
        })
}

fn scan<'a, U: AsRef<Path>>(
    src: &U,
    tx: Sender<(DirEntryResult, u64)>,
    scope: &Scope<'a>,
) {
    let dir = fs::read_dir(src).unwrap();
    dir.into_iter().for_each(|entry| {
        let info = entry.as_ref().unwrap();
        let path = info.path();

        if path.is_dir() {
            let tx = tx.clone();
            scope.spawn(move |s| scan(&path, tx, s))
        } else {
            // dbg!("{}", path.as_os_str().to_string_lossy());
            let size = info.metadata().unwrap().len();
            tx.send((entry, size)).unwrap();
        }
    });
}
