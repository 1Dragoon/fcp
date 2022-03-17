use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use indicatif::{FormattedDuration, HumanBytes, ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::Scope;
use std::fs::{self, DirEntry, Metadata};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::time::Duration;
use std::{io, thread};

mod win_stuff;

#[derive(Debug, Parser)]
#[clap(name = "fcp", about = "Multi-threaded copy...in rust!")]
struct Opt {
    /// Source directory
    source: PathBuf,

    /// Destination directory
    dest: PathBuf,
    // / Allow copying from encrypted location to unencrypted location
    // allow_efs_to_nonefs: bool;
}

trait PathExt {
    fn dunce_canonicalize(&self) -> io::Result<PathBuf>;
}

impl PathExt for Path {
    fn dunce_canonicalize(&self) -> io::Result<PathBuf> {
        dunce::canonicalize(&self)
    }
}

fn main() -> Result<()> {
    let args = Opt::parse();

    let src_str = args.source.to_str().context("Source path has non-standard UTF-8. Non-standard UTF-8 paths aren't supported at this time.")?;
    let dst_str = args.dest.to_str().context("Destination path has non-standard UTF-8. Non-standard UTF-8 paths aren't supported at this time.")?;

    let src_arg = shellexpand::full(src_str)
        .context("Unable to either look up or parse source path.")?
        .into_owned();
    let dst_arg = shellexpand::full(dst_str)
        .context("Unable to either look up or parse destination path.")?
        .into_owned();

    copy_recurse(&src_arg, &dst_arg)?;

    Ok(())
}

#[allow(clippy::option_map_unit_fn)]
fn copy_recurse<U: AsRef<Path>, V: AsRef<Path>>(source: &U, dest: &V) -> Result<()> {
    let start = std::time::Instant::now();

    let (source, dest) = normalize_input(source, dest)?;

    let pb = ProgressBar::new(0);
    pb.enable_steady_tick(50);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} ({eta}) [{msg}]",
            )
            .progress_chars("#>-")
            // .on_finish(finish)
    );
    // pb.enable_steady_tick(10);

    let state = work(source, &pb, dest);

    let successful_copy_sizes = state
        .complete_rx
        .into_iter()
        .filter_map(|r| match r {
            JobResult::CopySuccess(s) => Some(s.size),
            JobResult::ScanFileFailure(_) => None,
            JobResult::ScanDirFailure(_) => None,
            JobResult::ScanSuccess(_) => None,
            JobResult::ScanPartialSuccess(_) => None,
            JobResult::CopyFailure(_) => None,
            JobResult::CopyPartialSuccess(s) => s.size,
            JobResult::PermaFailure(s) => s.size,
        })
        .collect::<Vec<_>>(); // Block here until complete_tx sender is gone
    let copied_data = successful_copy_sizes.iter().sum();

    let perm_failed = state.failed_rx.into_iter().collect::<Vec<_>>(); // Block here until the err_thread sender is gone

    state.copy_thread.join().unwrap();
    state.err_thread.join().unwrap();
    pb.finish_at_current_pos();
    let _ = pb;

    if !perm_failed.is_empty() {
        perm_failed.into_iter().for_each(|r| {
            let err = match r {
                JobResult::ScanFileFailure(a) => Some(a.error),
                JobResult::ScanDirFailure(a) => Some(a.error),
                JobResult::ScanSuccess(_) => None,
                JobResult::ScanPartialSuccess(a) => Some(a.error),
                JobResult::CopyFailure(a) => Some(a.error),
                JobResult::CopySuccess(_) => None,
                JobResult::CopyPartialSuccess(_) => None,
                JobResult::PermaFailure(a) => Some(a.error),
            };

            err.map(|e| println!("{:?}", e));
        });
        println!(
            "Copied {} files of {}, {} of {} in {}",
            successful_copy_sizes.len(),
            state.file_count.load(Ordering::Relaxed),
            HumanBytes(copied_data),
            HumanBytes(state.total_size.load(Ordering::Relaxed)),
            FormattedDuration(start.elapsed())
        );
    } else {
        println!(
            "Copied {} files, {} in {}",
            state.file_count.load(Ordering::Relaxed),
            HumanBytes(state.total_size.load(Ordering::Relaxed)),
            FormattedDuration(start.elapsed())
        );
    }

    Ok(())
}

struct State {
    failed_rx: mpsc::Receiver<JobResult>,
    file_count: Arc<AtomicUsize>,
    total_size: Arc<AtomicU64>,
    complete_rx: mpsc::Receiver<JobResult>,
    copy_thread: thread::JoinHandle<()>,
    err_thread: thread::JoinHandle<()>,
}

fn work(source: PathBuf, pb: &ProgressBar, dest: PathBuf) -> State {
    let (failed_tx, failed_rx) = mpsc::channel();
    let file_count = Arc::new(AtomicUsize::new(0));
    let total_size = Arc::new(AtomicU64::new(0));
    let scan_finished = Arc::new(AtomicBool::new(false));
    let (scan_tx, scan_rx) = mpsc::channel();
    let scan_thread = {
        let ts = total_size.clone();
        let sf = scan_finished.clone();
        let src = source.clone();
        let scan_pb = pb.clone();
        thread::spawn(move || {
            rayon::scope(|s| scan(scan_pb, &src, scan_tx, ts, s));
            sf.store(true, Ordering::Relaxed);
        })
    };
    let (complete_tx, complete_rx) = mpsc::channel();
    let (error_tx, error_rx) = mpsc::channel();
    let copy_finished = Arc::new(AtomicBool::new(false));
    let copy_thread = {
        let etx = error_tx;
        let ftx = failed_tx.clone();
        let ctx = complete_tx.clone();
        let cp_fin = copy_finished.clone();
        let cp_pb = pb.clone();
        let cp_fc = file_count.clone();
        let cp_src = source.clone();
        let cp_dst = dest.clone();
        thread::spawn(move || {
            loop {
                let done = scan_finished.load(Ordering::Relaxed);
                thread::sleep(Duration::new(0, 100_000_000)); // Sleep 100ms to give the CPU a coffee break
                let scan_results = scan_rx.try_iter().into_iter().collect::<Vec<_>>();
                let mut file_set = Vec::new();
                for scan_result in scan_results {
                    match scan_result {
                        JobResult::ScanFileFailure(_) => ftx.send(scan_result).unwrap(),
                        JobResult::ScanDirFailure(_) => ftx.send(scan_result).unwrap(),
                        JobResult::ScanSuccess(_) => file_set.push(scan_result),
                        JobResult::ScanPartialSuccess(_) => file_set.push(scan_result),
                        JobResult::CopyFailure(_) => file_set.push(scan_result),
                        JobResult::CopySuccess(_) => ctx.send(scan_result).unwrap(),
                        JobResult::CopyPartialSuccess(_) => ctx.send(scan_result).unwrap(),
                        JobResult::PermaFailure(_) => ftx.send(scan_result).unwrap(),
                    }
                }
                if !file_set.is_empty() {
                    cp_fc.fetch_add(file_set.len(), Ordering::Relaxed);
                    process_files(&cp_pb, &cp_src, &cp_dst, file_set, etx.clone(), ctx.clone());
                }
                if done {
                    break;
                }
            }
            cp_fin.store(true, Ordering::Relaxed);
        })
    };
    // Let the scan finish before we start working on retries
    scan_thread.join().unwrap();
    let err_thread = {
        let ftx = failed_tx;
        let ctx = complete_tx;
        let err_pb = pb.clone();
        thread::spawn(move || {
            loop {
                let done = copy_finished.load(Ordering::Relaxed);
                thread::sleep(Duration::new(0, 100_000_000)); // Sleep 100ms to give the CPU a coffee break
                let copy_results = error_rx.try_iter().into_iter().collect::<Vec<_>>();
                let mut retry_set = Vec::new();
                for copy_result in copy_results {
                    match copy_result {
                        JobResult::ScanFileFailure(_) => ftx.send(copy_result).unwrap(),
                        JobResult::ScanDirFailure(_) => ftx.send(copy_result).unwrap(),
                        JobResult::ScanSuccess(_) => retry_set.push(copy_result),
                        JobResult::ScanPartialSuccess(_) => retry_set.push(copy_result),
                        JobResult::CopyFailure(_) => retry_set.push(copy_result),
                        JobResult::CopySuccess(_) => ctx.send(copy_result).unwrap(),
                        JobResult::CopyPartialSuccess(_) => ctx.send(copy_result).unwrap(),
                        JobResult::PermaFailure(_) => ftx.send(copy_result).unwrap(),
                    }
                }

                if !retry_set.is_empty() {
                    process_files(&err_pb, &source, &dest, retry_set, ftx.clone(), ctx.clone());
                }
                if done {
                    break;
                }
            }
        })
    };

    State {
        failed_rx,
        file_count,
        total_size,
        complete_rx,
        copy_thread,
        err_thread,
    }
}

fn normalize_input<U: AsRef<Path>, V: AsRef<Path>>(
    from: &U,
    to: &V,
) -> Result<(PathBuf, PathBuf), anyhow::Error> {
    // let options = MatchOptions {
    //     case_sensitive: false,
    //     require_literal_separator: false,
    //     require_literal_leading_dot: false,
    // };
    let src_str = from.as_ref().to_str().unwrap();
    let source = if is_wildcard_path(src_str) {
        // glob::glob_with(src_str, options)(|e| {
        //     expect!("Source used a glob pattern, but the pattern was invalid.");
        // })
        bail!("Wildcard source paths are not currently supported.")
    } else {
        PathBuf::from(src_str)
            .dunce_canonicalize()
            .context("Unable to resolve complete source path.")?
    };
    let dst_str = to.as_ref().to_str().unwrap();
    let dest = if is_wildcard_path(dst_str) {
        bail!("Wildcard destination paths are not allowed.")
    } else {
        let dpath = PathBuf::from(dst_str);
        fs::create_dir_all(&dpath).context("Unable to resolve complete destination path.")?;
        dpath
            .dunce_canonicalize()
            .context("Unable to resolve complete destination path.")?
    };
    if source == dest {
        bail!("Source and destination paths are the same.");
    }
    Ok((source, dest))
}

fn is_wildcard_path(src_str: &str) -> bool {
    src_str
        .chars()
        .any(|c| -> bool { c == '?' || c == '*' || c == '[' || c == ']' })
}

fn process_files<U: AsRef<Path> + Sync, V: AsRef<Path> + Sync>(
    pb: &ProgressBar,
    source: &U,
    dest: &V,
    files: Vec<JobResult>,
    failed_tx: Sender<JobResult>,
    complete_tx: Sender<JobResult>,
) {
    files
        .into_par_iter()
        .map_with((failed_tx, complete_tx), |senders, result| {
            (senders.clone(), result)
        })
        .for_each(|f| {
            let (senders, scan_result) = f;
            let (failed_tx, complete_tx) = senders;

            let mut ji = JobInfo::new();

            let can_process = match scan_result {
                JobResult::ScanFileFailure(_) => panic!(), // Shouldn't happen
                JobResult::ScanDirFailure(_) => panic!(),  // Shouldn't happen
                JobResult::ScanSuccess(a) => { ji.dir_entry = Some(a.dir_entry); ji.metadata = Some(a.meta_data); ji.has_md = true; ji.size = Some(a.size); true },
                JobResult::ScanPartialSuccess(a) => { ji.dir_entry = Some(a.dir_entry); true  }
                JobResult::CopyFailure(a) => { ji.dir_entry = Some(a.dir_entry); ji.error.extend(a.error); ji.metadata = a.meta_data; ji.has_md = true; ji.size = a.size; true },
                JobResult::CopySuccess(_) => panic!(), // Shouldn't happen
                JobResult::CopyPartialSuccess(_) => panic!(), // Shouldn't happen
                JobResult::PermaFailure(a) => { ji.dir_entry = a.dir_entry; ji.error.extend(a.error); ji.metadata = a.meta_data; ji.has_md = true; ji.size = a.size; false },
            };

            if can_process {
                let jobresult = copy_file(source, dest, &mut ji, pb);
                match jobresult {
                    Ok(r) => complete_tx.send(r).unwrap(),
                    Err(err) => 
                    { let error = vec![err];
                        failed_tx
                        .send(JobResult::CopyFailure(CopyFailure {
                            error,
                            dir_entry: ji.dir_entry.unwrap(),
                            meta_data: ji.metadata,
                            size: ji.size,
                        }))
                        .unwrap()},
                };
            } else {
                let jobresult = JobResult::PermaFailure(PermaFailure{
                    error: ji.error,
                    dir_entry: ji.dir_entry,
                    meta_data: ji.metadata,
                    size: ji.size,
                });
                failed_tx.send(jobresult).unwrap();
            }


        })
}

fn copy_file<U: AsRef<Path> + Sync, V: AsRef<Path> + Sync>(
    source: &U,
    dest: &V,
    process: &mut JobInfo,
    pb: &ProgressBar,
) -> Result<JobResult, anyhow::Error> {
    let spath = process.dir_entry.as_ref().unwrap().path();

    let s = spath.display().to_string();
    if let Some((i, _)) = s.char_indices().rev().nth(40) {
        pb.set_message(s[i..].to_owned());
    }
    let stem = spath.strip_prefix(&source)?;
    let dpath = dest.as_ref().join(stem);

    // If we don't have the size, make sure we have the metadata. If we don't have the metadata and still can't get it, leave both it and size to None.
    if process.size.is_none() {
        if let Some(metadata) = &process.metadata {
            process.has_md = true;
            process.size = Some(metadata.len());
        } else {
            process.metadata = process
                .metadata
                .take()
                .or_else(|| spath.metadata().ok())
                .map(|md| {
                    // If we can get the metadata, set both it and the size fields.
                    process.size = Some(md.len());
                    md
                });
            process.has_md = true;
        }
    }

    if dpath.exists() {
        let d_metadata = dpath.metadata()?;
        let mut permissions = d_metadata.permissions();
        if permissions.readonly() {
            permissions.set_readonly(false);
            fs::set_permissions(&dpath, permissions)?
        }
    } else {
        fs::create_dir_all(&dpath.parent().unwrap_or_else(|| dpath.as_path()))?;
    }

    #[cfg(target_os = "windows")]
    let transferred = win_stuff::win_copy(&spath, &dpath, pb)?;
    if process.size.is_some() {
        // let size = copy_result.size.unwrap();
        // if size != transferred {
        // println!("Liar!");
        // }
    } else {
        process.size = Some(transferred);
    }

    #[cfg(not(target_os = "windows"))]
    {
        fs::copy(&spath, &dpath)?;
        if process.size.is_some() {
            let size = process.size.unwrap();
            pb.inc(size);
        }
    }

    process.success = true;

    let jobresult = if process.has_md {
        JobResult::CopySuccess(CopySuccess {
            dir_entry: process.dir_entry.take().unwrap(),
            meta_data: process.metadata.take().unwrap(),
            size: process.size.unwrap(),
        })
    } else {
        JobResult::CopyPartialSuccess(CopyPartialSuccess {
            dir_entry: process.dir_entry.take().unwrap(),
            meta_data: process.metadata.take(),
            size: process.size,
        })
    };

    Ok(jobresult)
}

// Add backoff timer to copy failures? Re-send failed dir scans back into the scanner, also with backoff timer?
enum JobResult {
    ScanFileFailure(ScanFileFailure),
    ScanDirFailure(ScanDirFailure),
    ScanSuccess(ScanSuccess),
    ScanPartialSuccess(ScanPartialSuccess),
    CopyFailure(CopyFailure),
    CopySuccess(CopySuccess),
    CopyPartialSuccess(CopyPartialSuccess),
    PermaFailure(PermaFailure),
}

struct ScanPartialSuccess {
    error: Vec<anyhow::Error>,
    dir_entry: DirEntry,
}

struct CopyFailure {
    error: Vec<anyhow::Error>,
    dir_entry: DirEntry,
    meta_data: Option<Metadata>,
    size: Option<u64>,
}

struct PermaFailure {
    error: Vec<anyhow::Error>,
    dir_entry: Option<DirEntry>,
    meta_data: Option<Metadata>,
    size: Option<u64>,
}

struct CopySuccess {
    dir_entry: DirEntry,
    meta_data: Metadata,
    size: u64,
}
struct CopyPartialSuccess {
    dir_entry: DirEntry,
    meta_data: Option<Metadata>,
    size: Option<u64>,
}

struct ScanFileFailure {
    error: Vec<anyhow::Error>,
    dir_entry_result: Result<DirEntry, std::io::Error>,
}

struct ScanDirFailure {
    error: Vec<anyhow::Error>,
    dir: PathBuf,
}

struct ScanSuccess {
    dir_entry: DirEntry,
    meta_data: Metadata,
    size: u64,
}

struct JobInfo {
    error: Vec<anyhow::Error>,
    dir_entry: Option<DirEntry>,
    dir: Option<PathBuf>,
    metadata: Option<Metadata>,
    size: Option<u64>,
    success: bool,
    has_md: bool,
}

impl JobInfo {
    fn new() -> JobInfo {
        JobInfo {
            error: Vec::new(),
            dir_entry: None,
            dir: None,
            metadata: None,
            size: None,
            success: false,
            has_md: false,
        }
    }
}

fn scan<'a, U: AsRef<Path>>(
    pb: ProgressBar,
    src: &U,
    tx: Sender<JobResult>,
    total_size: Arc<AtomicU64>,
    scope: &Scope<'a>,
) {
    match fs::read_dir(src) {
        Ok(dir) => {
            dir.into_iter().for_each(|dir_entry_result| {
                // let mut result = ActionResult::new();
                match dir_entry_result {
                    Ok(dir_entry) => {
                        let path = dir_entry.path();
                        if path.is_dir() {
                            let tx = tx.clone();
                            let pb = pb.clone();
                            let ts = total_size.clone();
                            scope.spawn(move |s| scan(pb, &path, tx, ts, s))
                        } else {
                            match path.metadata() {
                                Ok(meta_data) => {
                                    let size = meta_data.len();
                                    pb.inc_length(size);
                                    total_size.fetch_add(size, Ordering::Relaxed);
                                    let result = JobResult::ScanSuccess(ScanSuccess {
                                        dir_entry,
                                        meta_data,
                                        size,
                                    });
                                    tx.send(result).unwrap();
                                }
                                Err(err) => {
                                    let error = vec![anyhow!("Failed to read metadata: {}", err)];
                                    let result =
                                        JobResult::ScanPartialSuccess(ScanPartialSuccess {
                                            error,
                                            dir_entry,
                                        });
                                    tx.send(result).unwrap();
                                }
                            }
                        }
                    }
                    Err(ref err) => {
                        let error = vec![anyhow!("Failed to get directory entry: {}", err)];
                        let result = JobResult::ScanFileFailure(ScanFileFailure {
                            error,
                            dir_entry_result,
                        });
                        tx.send(result).unwrap();
                    }
                }
            });
        }
        Err(err) => {
            let error = vec![anyhow!("Failed to read from directory: {}", err)];
            let dir = src.as_ref().to_path_buf();
            let result = JobResult::ScanDirFailure(ScanDirFailure { error, dir });
            tx.send(result).unwrap();
        }
    }
}
