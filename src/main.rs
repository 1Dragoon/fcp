use clap::Parser;
use indicatif::{FormattedDuration, HumanBytes, ProgressBar, ProgressStyle};
use rayon::iter::{Either, IntoParallelIterator, ParallelIterator};
use rayon::Scope;
use std::fs::{self, DirEntry};
use std::path::{Path, PathBuf};
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
    let total_size = sizes.into_iter().fold(0, |acc, f| acc + f);

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
    let sender = tx.clone();
    let perm_failed = process_files(&pb, &source, &dest, err_set, sender).1;
    let total_copied = rx.iter().count();
    // let copied_data = rx.into_iter().fold(0, |acc, x| acc + x);
    
    pb.finish_at_current_pos();

    perm_failed.into_iter().map(|f| f.1).for_each(|f| println!("{}", f));
    println!(
        "Copied {total_copied} files of {file_count}, {} in {}",
        HumanBytes(total_size),
        FormattedDuration(start.elapsed())
    );
}

fn process_files(
    pb: &ProgressBar,
    source: &PathBuf,
    dest: &PathBuf,
    files: Vec<Result<DirEntry, std::io::Error>>,
    tx: Sender<u64>
) -> (Vec<Result<DirEntry, std::io::Error>>, Vec<(Result<DirEntry, std::io::Error>, String)>) {
    // Won't ever get used, but it makes the compiler happy
    let default = PathBuf::from(".");

    files
        .into_par_iter().map_with(tx, |s, x| (s.clone(), x))
        .partition_map(|f: (Sender<u64>, Result<DirEntry, std::io::Error>)| {
            let (tx, file) = f;
            let entry = match file {
                Ok(ref ok) => ok,
                Err(ref err) => {
                    let err_message = format!("Error: {:?}", err.kind());
                    std::mem::drop(tx);
                    return Either::Right((file, err_message));
                }
            };
            let spath = entry.path();
            pb.set_message(format!("{}", spath.display()));
            let stem = match spath.strip_prefix(&source) {
                Ok(ok) => ok,
                Err(ref err) => {
                    let err_message = format!(
                        "Strip Prefix Error: {} for file {}",
                        err.to_string(),
                        entry.path().as_os_str().to_string_lossy()
                    );
                    std::mem::drop(tx);
                    return Either::Right((file, err_message));
                }
            };
            let dpath = dest.join(&stem);
            let size = match entry.metadata() {
                Ok(ok) => ok,
                Err(ref err) => {
                    let err_message = format!(
                        "Metadata Fetch Error: {:?} for file {}",
                        err.kind(),
                        entry.path().as_os_str().to_string_lossy()
                    );
                    std::mem::drop(tx);
                    return Either::Right((file, err_message));
                }
            }
            .len();
            match fs::create_dir_all(&dpath.parent().unwrap_or_else(|| default.as_path())) {
                Ok(_) => {}
                Err(ref err) => {
                    let err_message = format!(
                        "Directory Create Error: {:?} for file {}",
                        err.kind(),
                        entry.path().as_os_str().to_string_lossy()
                    );
                    std::mem::drop(tx);
                    return Either::Right((file, err_message));
                }
            };
            match fs::copy(&spath, &dpath) {
                Ok(_) => {}
                Err(ref err) => {
                    let err_message = format!(
                        "Error while copying: {:?} for file {}",
                        err.kind(),
                        entry.path().as_os_str().to_string_lossy()
                    );
                    std::mem::drop(tx);
                    return Either::Right((file, err_message));
                }
            };
            tx.send(size).unwrap();
            pb.inc(size);
            Either::Left(file)
        })
}

fn scan<'a, U: AsRef<Path>>(
    src: &U,
    tx: Sender<(Result<DirEntry, std::io::Error>, u64)>,
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
