// #![feature(async_closure)]

use anyhow::{anyhow, bail, Context, Result};
use async_recursion::async_recursion;
use clap::Parser;
use futures::future;
use futures::{StreamExt, future::ready};
use indicatif::{FormattedDuration, HumanBytes, ProgressBar, ProgressStyle};
use std::fs::Metadata;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::fs::{self, DirEntry};
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio::task::JoinHandle;
use tokio::time::{self, Duration, Instant};
use tokio::{io};
use tokio_stream::wrappers::{ReadDirStream, UnboundedReceiverStream};
use tokio_stream::StreamExt as TokioStreamExt;

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

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let args = Opt::parse();

    let src_str = args.source.to_str().context("Source path has non-standard UTF-8. Non-standard UTF-8 paths aren't supported at this time.")?;
    let dst_str = args.dest.to_str().context("Destination path has non-standard UTF-8. Non-standard UTF-8 paths aren't supported at this time.")?;

    let src_arg = shellexpand::full(src_str)
        .context("Unable to either look up or parse source path.")?
        .into_owned();
    let dst_arg = shellexpand::full(dst_str)
        .context("Unable to either look up or parse destination path.")?
        .into_owned();

    copy_recurse(&src_arg, &dst_arg).await?;

    Ok(())
}

async fn copy_recurse<U: AsRef<Path>, V: AsRef<Path>>(
    source: &U,
    dest: &V,
) -> Result<(), anyhow::Error> {
    let clock = Arc::new(Instant::now());

    let (source, dest) = normalize_input(source, dest).await?;
    let source = Arc::new(source);
    // let dest = Arc::new(dest);

    let pb = ProgressBar::new(0);
    pb.enable_steady_tick(Duration::from_millis(50));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} ({eta}) [{wide_msg:>}]",
            ).expect("Bad programmer! Bad!")
            .progress_chars("#>-")
            // .on_finish(finish)
    );
    // pb.enable_steady_tick(10);

    let state = work(source, dest, pb.clone(), clock.clone()).await;

    let successful_copy_sizes =
        TokioStreamExt::collect::<Vec<_>>(UnboundedReceiverStream::from(state.complete_rx))
            .await
            .into_iter()
            .filter_map(|r| match r {
                JobStatus::CopySuccess(s) => s.meta_data.map(|md| md.len()),
                JobStatus::ScanFailure(_) => None,
                JobStatus::ScanSuccess(_) => None,
                JobStatus::CopyFailure(_) => None,
                JobStatus::PermaFailure(_) => None,
            })
            .collect::<Vec<_>>(); // Block here until the err_thread sender is gone
    let copied_data = successful_copy_sizes.iter().sum();

    let perm_failed =
        tokio_stream::StreamExt::collect::<Vec<_>>(UnboundedReceiverStream::from(state.failed_rx))
            .await; // Block here until the err_thread sender is gone

    state.scan_task.await.unwrap();
    state.copy_task.await.unwrap();
    // state.err_task.await.unwrap();
    // pb.finish_at_current_pos();
    std::mem::drop(pb);

    if !perm_failed.is_empty() {
        perm_failed.into_iter().for_each(|r| {
            let err = match r {
                JobStatus::ScanFailure(a) => Some(a.error),
                JobStatus::ScanSuccess(_) => None,
                JobStatus::CopyFailure(a) => Some(a.error),
                JobStatus::CopySuccess(a) => Some(a.error),
                JobStatus::PermaFailure(a) => Some(a.error),
            };

            if let Some(e) = err {
                println!("{:?}", e)
            }
        });
        println!(
            "Copied {} files of {}, {} of {} in {}",
            successful_copy_sizes.len(),
            state.file_count.load(Ordering::Relaxed),
            HumanBytes(copied_data),
            HumanBytes(state.total_size.load(Ordering::Relaxed)),
            FormattedDuration(clock.elapsed())
        );
    } else {
        println!(
            "Copied {} files, {} in {}",
            state.file_count.load(Ordering::Relaxed),
            HumanBytes(state.total_size.load(Ordering::Relaxed)),
            FormattedDuration(clock.elapsed())
        );
    }

    Ok(())
}

struct State {
    failed_rx: UnboundedReceiver<JobStatus>,
    file_count: Arc<AtomicUsize>,
    total_size: Arc<AtomicU64>,
    complete_rx: UnboundedReceiver<JobStatus>,
    scan_task: JoinHandle<()>,
    copy_task: JoinHandle<()>,
    // err_thread: JoinHandle<()>,
}

async fn work(source: Arc<PathBuf>, dest: PathBuf, pb: ProgressBar, clock: Arc<Instant>) -> State {
    let (failed_tx, failed_rx) = mpsc::unbounded_channel();
    let file_count = Arc::new(AtomicUsize::new(0));
    let total_size = Arc::new(AtomicU64::new(0));
    let scan_finished = Arc::new(AtomicBool::new(false));
    let (scan_tx, scan_rx) = mpsc::unbounded_channel();
    let ts = total_size.clone();
    let sf = scan_finished.clone();
    let src = source.clone();
    let scan_pb = pb.clone();
    let sc_tx = scan_tx;
    let scan_task = tokio::spawn(async move {
        scan(scan_pb, src, sc_tx, ts).await;
        sf.store(true, Ordering::Relaxed);
    });
    let (complete_tx, complete_rx) = mpsc::unbounded_channel();
    let copy_finished = Arc::new(AtomicBool::new(false));
    let copy_task = {
        let ftx = failed_tx;
        let ctx = complete_tx;
        // let sctx = scan_tx;
        let cp_fin = copy_finished;
        let cp_pb = pb;
        let cp_fc = file_count.clone();
        let cp_src = source;
        let cp_dst = dest;
        tokio::spawn(async {
            main_work(
                scan_finished,
                scan_rx,
                ftx,
                // sctx,
                ctx,
                cp_fc,
                cp_pb,
                cp_src,
                cp_dst,
                cp_fin,
                clock,
            )
            .await;
        })
    };

    State {
        failed_rx,
        file_count,
        total_size,
        complete_rx,
        scan_task,
        copy_task,
        // err_thread,
    }
}

async fn main_work(
    _scan_finished: Arc<AtomicBool>,
    mut scan_rx: UnboundedReceiver<JobStatus>,
    failed_tx: UnboundedSender<JobStatus>,
    // scan_tx: UnboundedSender<JobStatus>,
    complete_tx: UnboundedSender<JobStatus>,
    file_count: Arc<AtomicUsize>,
    pb: ProgressBar,
    source: Arc<PathBuf>,
    dest: PathBuf,
    copy_finished: Arc<AtomicBool>,
    clock: Arc<Instant>,
) {
    // let mut empty = 0;
    let mut op_file_count = 0;
    let mut done = false;
    let mut jobs = Vec::new();
    loop {
        // let mut done = scan_finished.load(Ordering::Relaxed);
        time::sleep(Duration::new(0, 100_000_000)).await; // Sleep 100ms to give the CPU a coffee break
        let mut files = Vec::new();
        loop {
            match scan_rx.try_recv() {
                Ok(job_status) => jobs.push(job_status),
                Err(err) => match err {
                    mpsc::error::TryRecvError::Empty => {
                        break;
                    }
                    mpsc::error::TryRecvError::Disconnected => {
                        done = true;
                        break;
                    }
                },
            }
        }

        let mut wait = Vec::new();
        op_file_count += jobs.len();
        for mut job_status in jobs.drain(0..jobs.len()) {
            //.try_iter().into_iter().collect::<Vec<_>>();
            match job_status {
                JobStatus::ScanFailure(_) => failed_tx.send(job_status).unwrap(),
                JobStatus::ScanSuccess(_) => {
                    files.push(job_status);
                }
                JobStatus::CopyFailure(ref mut job) => {
                    if job.try_count > 2 {
                        let mut error = Vec::new();
                        error.append(&mut job.error);
                        let fail = JobStatus::PermaFailure(Box::new(PermaFailure {
                            error,
                            dir_entry: job.dir_entry.take(),
                            meta_data: job.meta_data.take(),
                        }));
                        failed_tx.send(fail).unwrap();
                    } else if clock.elapsed().as_secs() >= job.retry_at {
                        wait.push(job_status);
                    } else {
                        files.push(job_status);
                    }
                }
                JobStatus::CopySuccess(_) => complete_tx.send(job_status).unwrap(),
                JobStatus::PermaFailure(_) => failed_tx.send(job_status).unwrap(),
            }
        }

        let tasks = files
            .into_iter()
            .map(|job| {
                process_file(
                    pb.clone(),
                    source.clone(),
                    &dest,
                    job,
                    failed_tx.clone(),
                    complete_tx.clone(),
                    clock.clone(),
                )
            })
            .collect::<Vec<_>>();
        jobs.extend(wait);
        jobs.extend(futures::future::join_all(tasks).await.into_iter().flatten());

        if done && jobs.is_empty() {
            break;
        }
    }
    file_count.fetch_add(op_file_count, Ordering::Relaxed);
    copy_finished.store(true, Ordering::Relaxed);
}

async fn normalize_input<U: AsRef<Path>, V: AsRef<Path>>(
    from: &U,
    to: &V,
) -> Result<(PathBuf, PathBuf), anyhow::Error> {
    // let options = MatchOptions {
    //     case_sensitive: false,
    //     require_literal_separator: false,
    //     require_literal_leading_dot: false,
    // };
    let src_str = from.as_ref().to_str().unwrap();
    let source = if is_wildcard_path(src_str).await {
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
    let dest = if is_wildcard_path(dst_str).await {
        bail!("Wildcard destination paths are not allowed.")
    } else {
        let dpath = PathBuf::from(dst_str);
        fs::create_dir_all(&dpath)
            .await
            .context("Unable to resolve complete destination path.")?;
        dpath
            .dunce_canonicalize()
            .context("Unable to resolve complete destination path.")?
    };
    if source == dest {
        bail!("Source and destination paths are the same.");
    }
    Ok((source, dest))
}

async fn is_wildcard_path(src_str: &str) -> bool {
    src_str
        .chars()
        .any(|c| -> bool { c == '?' || c == '*' || c == '[' || c == ']' })
}

async fn process_file<V: AsRef<Path> + Sync + Send>(
    pb: ProgressBar,
    source: Arc<PathBuf>,
    dest: &V,
    job: JobStatus,
    failed_tx: UnboundedSender<JobStatus>,
    complete_tx: UnboundedSender<JobStatus>,
    clock: Arc<Instant>,
) -> Option<JobStatus> {
    let mut recycle = None;
    let mut scratch_pad = ScratchPad::new();

    let can_process = match job {
        JobStatus::ScanFailure(_) => panic!(), // Shouldn't happen
        JobStatus::ScanSuccess(a) => {
            scratch_pad.attempt = 0;
            scratch_pad.dir_entry = Some(a.dir_entry);
            scratch_pad.error.extend(a.error);
            scratch_pad.metadata = a.meta_data;
            true
        }
        JobStatus::CopyFailure(a) => {
            scratch_pad.attempt = a.try_count;
            scratch_pad.dir_entry = a.dir_entry;
            scratch_pad.error.extend(a.error);
            scratch_pad.metadata = a.meta_data;
            true
        }
        JobStatus::CopySuccess(_) => panic!(), // Shouldn't happen
        JobStatus::PermaFailure(a) => {
            scratch_pad.dir_entry = a.dir_entry;
            scratch_pad.error.extend(a.error);
            scratch_pad.metadata = a.meta_data;
            false
        }
    };

    if can_process {
        let jobresult = copy_file(source, dest, &mut scratch_pad, &pb).await;
        match jobresult {
            Ok(r) => complete_tx.send(r).unwrap(),
            Err(err) => {
                let error = vec![err];
                let jr = JobStatus::CopyFailure(Box::new(CopyFailure {
                    error,
                    dir_entry: scratch_pad.dir_entry,
                    meta_data: scratch_pad.metadata,
                    try_count: scratch_pad.attempt + 1,
                    retry_at: (clock.elapsed() + Duration::from_secs(2)).as_secs(),
                }));
                recycle = Some(jr);
            }
        };
    } else {
        let jobresult = JobStatus::PermaFailure(Box::new(PermaFailure {
            error: scratch_pad.error,
            dir_entry: scratch_pad.dir_entry,
            meta_data: scratch_pad.metadata,
        }));
        failed_tx.send(jobresult).unwrap();
    }
    recycle
}

async fn copy_file<U: AsRef<Path> + Sync, V: AsRef<Path> + Sync>(
    source: Arc<U>,
    dest: &V,
    process: &mut ScratchPad,
    pb: &ProgressBar,
) -> Result<JobStatus, anyhow::Error> {
    let spath = process.dir_entry.as_ref().unwrap().path();

    pb.set_message(spath.display().to_string());
    let stem = spath.strip_prefix(&*source)?;
    let dpath = dest.as_ref().join(stem);

    // If we don't have the size, make sure we have the metadata. If we don't have the metadata and still can't get it, leave both it and size to None.
    if process.size.is_none() {
        if let Some(metadata) = &process.metadata {
            process.size = Some(metadata.len());
        } else {
            process.metadata = process
                .metadata
                .take()
                .or_else(|| spath.metadata().map(Box::new).ok())
                .map(|md| {
                    // If we can get the metadata, set both it and the size fields.
                    process.size = Some(md.len());
                    md
                });
        }
    }

    if dpath.exists() {
        let d_metadata = dpath.metadata()?;
        let mut permissions = d_metadata.permissions();
        if permissions.readonly() {
            permissions.set_readonly(false);
            fs::set_permissions(&dpath, permissions)
                .await
                .context("Could not remove read-only flag")?;
        }
    } else {
        fs::create_dir_all(&dpath.parent().unwrap_or(dpath.as_path()))
            .await
            .context("Unable to create destination path")?;
    }

    #[cfg(target_os = "windows")]
    let transferred = win_stuff::win_copy(&spath, &dpath, pb).await?;
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
        fs::copy(&spath, &dpath)
            .await
            .context("Failed to copy file")?;
        if process.size.is_some() {
            let size = process.size.unwrap();
            pb.inc(size);
        }
    }

    process.success = true;

    let jobresult = {
        let mut error = Vec::new();
        error.append(&mut process.error);
        JobStatus::CopySuccess(Box::new(CopySuccess {
            error,
            meta_data: process.metadata.take(),
        }))
    };

    Ok(jobresult)
}

// Add backoff timer to copy failures? Re-send failed dir scans back into the scanner, also with backoff timer?
#[derive(Debug)]
enum JobStatus {
    ScanFailure(Box<ScanFailure>),
    ScanSuccess(Box<ScanSuccess>),
    CopyFailure(Box<CopyFailure>),
    CopySuccess(Box<CopySuccess>),
    PermaFailure(Box<PermaFailure>),
}

#[derive(Debug)]
struct ScanSuccess {
    error: Vec<anyhow::Error>,
    dir_entry: Box<DirEntry>,
    meta_data: Option<Box<Metadata>>,
}

#[derive(Debug)]
struct CopyFailure {
    error: Vec<anyhow::Error>,
    try_count: u8,
    retry_at: u64,
    dir_entry: Option<Box<DirEntry>>,
    meta_data: Option<Box<Metadata>>,
}

#[derive(Debug)]
struct PermaFailure {
    error: Vec<anyhow::Error>,
    dir_entry: Option<Box<DirEntry>>,
    meta_data: Option<Box<Metadata>>,
}

#[derive(Debug)]
struct CopySuccess {
    error: Vec<anyhow::Error>,
    meta_data: Option<Box<Metadata>>,
}

#[derive(Debug)]
enum ScanFailType {
    Directory(Box<PathBuf>),
    File(Box<Result<DirEntry, io::Error>>),
}

#[derive(Debug)]
struct ScanFailure {
    error: Vec<anyhow::Error>,
    kind: ScanFailType,
}

#[derive(Debug)]
struct ScratchPad {
    error: Vec<anyhow::Error>,
    attempt: u8,
    dir_entry: Option<Box<DirEntry>>,
    dir: Option<Box<PathBuf>>,
    metadata: Option<Box<Metadata>>,
    size: Option<u64>,
    success: bool,
}

impl ScratchPad {
    fn new() -> ScratchPad {
        ScratchPad {
            error: Vec::new(),
            attempt: 0,
            dir_entry: None,
            dir: None,
            metadata: None,
            size: None,
            success: false,
        }
    }
}

#[async_recursion]
async fn scan(
    pb: ProgressBar,
    src: Arc<PathBuf>,
    tx: UnboundedSender<JobStatus>,
    total_size: Arc<AtomicU64>,
) {
    let read_dir = fs::read_dir(&*src);
    let mut subtasks = Vec::new();
    let mut stream = None;
    let scan = match read_dir.await {
        Ok(k) => {
            stream = Some(ReadDirStream::new(k).for_each_concurrent(8,|dir_entry_result|{
                match dir_entry_result {
                    Ok(dir_entry) => {
                        let path = dir_entry.path();
                        if path.is_dir() {
                            let tx = tx.clone();
                            let pb = pb.clone();
                            let ts = total_size.clone();
                            subtasks.push(tokio::spawn(scan(pb, Arc::new(path), tx, ts)));
                            ready(())
                        } else {
                            match path.metadata() {
                                Ok(meta_data) => {
                                    let size = meta_data.len();
                                    pb.inc_length(size);
                                    total_size.fetch_add(size, Ordering::Relaxed);
                                    let result = JobStatus::ScanSuccess(Box::new(ScanSuccess {
                                        error: Vec::new(),
                                        dir_entry: Box::new(dir_entry),
                                        meta_data: Some(Box::new(meta_data)),
                                    }));
                                    tx.send(result).unwrap();
                                    ready(())
                                }
                                Err(err) => {
                                    let error = vec![anyhow!("Failed to read metadata: {err}")];
                                    let result = JobStatus::ScanSuccess(Box::new(ScanSuccess {
                                        error,
                                        dir_entry: Box::new(dir_entry),
                                        meta_data: None,
                                    }));
                                    tx.send(result).unwrap();
                                    ready(())
                                }
                            }
                        }
                    }
                    Err(ref err) => {
                        let error = vec![anyhow!("Failed to read from directory: {err}")];
                        let kind = ScanFailType::File(Box::new(dir_entry_result));
                        let result = JobStatus::ScanFailure(Box::new(ScanFailure { error, kind }));
                        tx.send(result).unwrap();
                        ready(())
                    }
                }
            }));
            ready(())
        }
        Err(err) => {
            let error = vec![anyhow!("Failed to read from directory: {err}")];
            let kind = ScanFailType::Directory(Box::new(src.as_ref().to_path_buf()));
            let result = JobStatus::ScanFailure(Box::new(ScanFailure { error, kind }));
            tx.send(result).unwrap();
            ready(())
        }
    };

    // ghetto scoping
    if let Some(s) = stream {
        futures::future::join(scan,  s).await;
    } else {
        scan.await;
    }
    future::join_all(subtasks).await;
}
