use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle, HumanBytes, FormattedDuration};
use rayon::Scope;
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use rayon::iter::{ParallelIterator, IntoParallelIterator, Either};
use std::sync::mpsc::{self, Sender};

#[derive(Debug, Parser)]
#[clap(name = "mtcopy", about = "Multi-threaded copy...in rust!")]
struct Opt {
    /// Source directory
    source: String,

    /// Destination directory
    dest: String,
}

fn main() {
    let args = Opt::parse();

    let source = PathBuf::from(&args.source).canonicalize().unwrap_or_default().as_os_str().to_string_lossy().into_owned();
    let dest = PathBuf::from(&args.dest).canonicalize().unwrap_or_default().as_os_str().to_string_lossy().into_owned();
    if source == dest {
        println!("Source and destination are the same.");
        return
    }

    copy(&args.source, &args.dest);
}

fn copy<U: AsRef<Path>, V: AsRef<Path>>(from: &U, to: &V) {

    let start = std::time::Instant::now();
    let source = PathBuf::from(from.as_ref());
    let dest = PathBuf::from(to.as_ref());

    let total_size = AtomicU64::new(0);
    let (tx, rx) = mpsc::channel();
    rayon::scope(|s| scan(&source, tx, s, &total_size));
    let files = rx.into_iter().collect::<Vec<_>>();
    let file_count = files.len();
    let total_size = total_size.into_inner();

    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {bytes}/{total_bytes} ({eta})",
            )
            .progress_chars("#>-")
            // .on_finish(finish)
    );

    // Won't ever get used, but it makes the compiler happy
    let default = PathBuf::from(".");

    // let (tx, rx) = mpsc::channel();
    let p = process_files(&pb, default, source, dest, files);
    let (_, err_set) = p;

    pb.finish_at_current_pos();

    println!("Copied {} files, {} in {}",  file_count, HumanBytes(total_size), FormattedDuration(start.elapsed()));
}

fn process_files(pb: &ProgressBar, default: PathBuf, source: PathBuf, dest: PathBuf, files: Vec<Result<DirEntry, std::io::Error>>) -> (Vec<()>, Vec<Result<DirEntry, std::io::Error>>) {
    let p: (Vec<()>, Vec<Result<DirEntry, std::io::Error>>) = files.into_par_iter().partition_map(|file: Result<DirEntry, std::io::Error> | {
        let entry = match file {
            Ok(ref ok) => ok,
            Err(_) => return Either::Right(file)
        };
        let spath = entry.path();
        pb.set_message(format!("{}", spath.display()));
        let stem = match spath.strip_prefix(&source) {
            Ok(ok) => ok,
            Err(_) => return Either::Right(file)
        };
        let dpath = dest.join(&stem);
        let size = match entry.metadata() {
            Ok(ok) => ok,
            Err(_) => return Either::Right(file)
        }.len();
        match fs::create_dir_all(&dpath.parent().unwrap_or_else(|| default.as_path())) {
            Ok(_) => {},
            Err(_) => return Either::Right(file)
        };
        match fs::copy(&spath, &dpath) {
            Ok(_) => {},
            Err(_) => return Either::Right(file)
        };
        pb.inc(size);
        Either::Left(())
    });
    p
}

fn scan<'a, U: AsRef<Path>>(src: &U, tx: Sender<Result<DirEntry, std::io::Error>>, s: &Scope<'a>, total_size: &'a AtomicU64) {
    let dir = fs::read_dir(src).unwrap();
    let mut dirsize = 0;
    dir.into_iter().for_each(|entry| {
        let info = entry.as_ref().unwrap();
        let path = info.path();

        if path.is_dir() {
            let tx = tx.clone();
            s.spawn(move |s| scan(&path, tx, s, total_size))
        } else {
            // println!("{}", path.as_os_str().to_string_lossy());
            dirsize += info.metadata().unwrap().len();
            tx.send(entry).unwrap();
        }
    });
    total_size.fetch_add(dirsize, Ordering::Relaxed);
}
