use clap::{AppSettings, Parser};
use indicatif::{ProgressBar, ProgressStyle, HumanBytes, FormattedDuration};
use rayon::Scope;
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use crossbeam::queue::SegQueue;
use rayon::iter::{ParallelIterator, IntoParallelRefIterator};

#[derive(Debug, Parser)]
#[clap(name = "mtcopy", about = "Multi-threaded copy...in rust!", setting = AppSettings::AllowNegativeNumbers)]
struct Opt {
    /// Source directory
    source: String,

    /// Destination directory
    dest: String,
}

fn main() {
    let args = Opt::parse();
    copy(&args.source, &args.dest);
}

fn copy<U: AsRef<Path>, V: AsRef<Path>>(from: &U, to: &V) {

    let start = std::time::Instant::now();
    let source = PathBuf::from(from.as_ref());
    let dest = PathBuf::from(to.as_ref());

    let total_size = AtomicU64::new(0);
    // Could possibly use dashmap for the bag instead, slower but it can be used to avoid duplicate files from e.g. symlinks
    let bag = SegQueue::new();
    rayon::scope(|s| scan(&source, &bag, s, &total_size));

    // Convert to a vec so that we can rayon it and throw the bag away
    let files = bag.into_iter().collect::<Vec<_>>();
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

    files.par_iter().for_each(|entry| {
        let entry = entry.as_ref().unwrap();
        let spath = entry.path();
        pb.set_message(format!("{}", spath.display()));
        let stem = spath.strip_prefix(&source).unwrap();
        let dpath = dest.join(&stem);
        let size = entry.metadata().unwrap().len();
        fs::create_dir_all(&dpath.parent().unwrap()).unwrap();
        // println!("  copy: {:?} -> {:?}", &path, &dpath);
        fs::copy(&spath, &dpath).unwrap();
        pb.inc(size);
    });

    pb.finish_at_current_pos();

    println!("Copied {} files, {} in {}",  files.len(), HumanBytes(total_size), FormattedDuration(start.elapsed()));
}

fn scan<'a, U: AsRef<Path>>(src: &U, bag: &'a SegQueue<Result<DirEntry, std::io::Error>>, s: &Scope<'a>, total_size: &'a AtomicU64) {
    let dir = fs::read_dir(src).unwrap();
    dir.into_iter().for_each(|entry| {
        let info = entry.as_ref().unwrap();
        let path = info.path();

        if path.is_dir() {
            s.spawn(move |s| scan(&path, bag, s, total_size))
        } else {
            // println!("{}", path.as_os_str().to_string_lossy());
            let filelength = info.metadata().unwrap().len();
            total_size.fetch_add(filelength, Ordering::SeqCst);
            bag.push(entry)
        }
    })
}
