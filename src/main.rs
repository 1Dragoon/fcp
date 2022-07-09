use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use indicatif::style::ProgressTracker;
use indicatif::{FormattedDuration, HumanBytes, ProgressBar, ProgressStyle, BinaryBytes, ProgressState, HumanDuration};
use number_prefix::NumberPrefix;
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::Scope;
use core::fmt;
use std::fmt::Write;
use std::fs::{self, DirEntry, Metadata};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::time::{Duration, Instant};
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

fn main() -> Result<(), anyhow::Error> {
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
fn copy_recurse<U: AsRef<Path>, V: AsRef<Path>>(source: &U, dest: &V) -> Result<(), anyhow::Error> {
    let start = Arc::new(Instant::now());

    let (source, dest) = normalize_input(source, dest)?;

    let pb = ProgressBar::new(0);
    pb.enable_steady_tick(Duration::from_millis(100));
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {bytes}/{total_bytes} {binary_bytes_per_sec} ({my_eta}) [{wide_msg:>}]",
            ).expect("Bad programmer! Bad!")
            .progress_chars("#>-").with_key("my_eta", |s: &ProgressState, w: &mut dyn Write| 
            match (s.pos(), s.len()){
               (0, _) => write!(w, "-").unwrap(),
               (pos,len) => write!(w, "{:#}", HumanDuration(Duration::from_secs(s.elapsed().as_secs() * (len.unwrap_or_default()-pos)/pos))).unwrap(),
           })
           .with_key("binary_bytes_per_sec", MyFormatter::new(3, Duration::from_millis(500)))

    );

    let state = work(source, dest, &pb);

    let successful_copy_sizes = state
        .complete_rx
        .into_iter()
        .filter_map(|r| match r {
            JobResult::CopySuccess(s) => s.meta_data.map(|md| md.len()),
            JobResult::ScanFailure(_) => None,
            JobResult::ScanSuccess(_) => None,
            JobResult::CopyFailure(_) => None,
            JobResult::PermaFailure(_) => None,
        })
        .collect::<Vec<_>>(); // Block here until complete_tx sender is gone
    let copied_data = successful_copy_sizes.iter().sum();

    let perm_failed = state.failed_rx.into_iter().collect::<Vec<_>>(); // Block here until the err_thread sender is gone

    state.copy_thread.join().unwrap();
    state.err_thread.join().unwrap();
    pb.finish();

    if !perm_failed.is_empty() {
        perm_failed.into_iter().for_each(|r| {
            let err = match r {
                JobResult::ScanFailure(a) => Some(a.error),
                JobResult::ScanSuccess(_) => None,
                JobResult::CopyFailure(a) => Some(a.error),
                JobResult::CopySuccess(_) => None,
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

#[derive(Debug, Clone)]
struct MyFormatter {
    // These provide functionality for the replacement estimator. We completely ignore the built-in one.
    samples: Box<[f64]>, // Variable sized circular buffer that holds previous rate samples
    prev_sample: (u64, Instant), // The previous state for computing the current rate
    sample_pos: usize,   // The current position in the circular buffer
    size: usize,         // The size of the circular buffer
    interval_period: Duration, // The period over which to calculate the rate
    full: bool,          // Whether the circular buffer is full
    rate: f64,           // The current rate calculated over the user defined period

    // These provide functionality for determining whether to override the SI scale
    prev_scale: u8,             // The previous scale of the rate
    downscale_backoff: Instant, // When to start downscaling
    force_upscale: bool,        // Whether to force an upscaling

                                // // These are only needed for calculating ETA time because Display doesn't have access to `ProgressState`
                                // bar_pos: u64, // The current position in the progress bar, needs to be stored for display to access it.
                                // bar_len: u64, // The length of the progress bar
}

/// Calculates rate of change of pos over last n samples. The more samples specified, the more data points needed, and the more memory needed.
impl MyFormatter {
    fn new(interval_count: usize, interval_period: Duration) -> Self {
        let mut size = interval_count;
        if size == 0 {
            // If the user screws up this is still safe and the cost is trivial
            size += 1;
        }
        Self {
            prev_scale: 0,
            downscale_backoff: Instant::now(),
            force_upscale: false,
            rate: 0.0,
            samples: vec![0.0; size].into_boxed_slice(),
            prev_sample: (0, Instant::now()),
            sample_pos: 0,
            interval_period,
            size,
            full: false,
        }
    }

    const fn len(&self) -> usize {
        if self.full {
            self.size
        } else {
            self.sample_pos
        }
    }

    // Acts as a substitute for the built in estimator. This one allows user defined sampling for smoother display.
    fn estimator(&mut self, state: &ProgressState, inst: Instant) {
        let (prev_pos, prev_instant) = &self.prev_sample;
        let delta = state.pos().saturating_sub(*prev_pos);
        if delta == 0 || inst <= *prev_instant {
            return; // No change in position, or time has not advanced since last checking
        }
        let elapsed = inst - *prev_instant;
        if elapsed < self.interval_period {
            return; // Only record a new sample after the user defined interval has passed
        }
        self.sample_pos = (self.sample_pos + 1) % self.size;
        self.samples[self.sample_pos] = delta as f64 / elapsed.as_secs_f64();
        self.prev_sample = (state.pos(), inst);
        let rates = self.samples.iter().sum::<f64>(); // Everything we haven't yet written to will just be zero.
        if !self.full && self.sample_pos == 0 {
            self.full = true;
        }
        self.rate = rates / self.len() as f64;
    }

    // Prevents the SI scale from changing back and forth too quickly by adding a backoff period when it does change.
    // For example, if the rate slows to 999KiB/s, it display as 0.99MiB/s, holding off a unit downscale for at least 3 seconds.
    fn si_scale_override(&mut self, inst: Instant) {
        let mut rate = self.rate as u64;
        let prev_scale = self.prev_scale;
        let mut prefix = 0_u8;
        while rate >= 1024 && prefix < 8 {
            rate /= 1024;
            prefix += 1;
        }
        if prefix < prev_scale || rate > 999 {
            self.downscale_backoff = inst;
            self.force_upscale = true;
        }
        let backoff_exceeded = if inst.gt(&self.downscale_backoff) {
            inst - self.downscale_backoff > Duration::from_secs(3)
        } else {
            false
        };
        if (prefix > prev_scale || backoff_exceeded) && rate < 1000 {
            self.force_upscale = false;
        }
        self.prev_scale = prefix;
    }
}

impl ProgressTracker for MyFormatter {
    fn clone_box(&self) -> std::boxed::Box<(dyn ProgressTracker + 'static)> {
        Box::new(self.clone())
    }

    fn tick(&mut self, state: &ProgressState, inst: Instant) {
        self.estimator(state, inst);
        self.si_scale_override(inst);
    }

    fn reset(&mut self, state: &ProgressState, now: Instant) {
        self.prev_scale = 0;
        self.downscale_backoff = now;
        self.force_upscale = false;
        self.samples = vec![0.0; self.size].into_boxed_slice();
        self.prev_sample = (state.pos(), now);
        self.samples[0] = self.rate;
        self.sample_pos = 0;
        self.rate = 0.0;
        self.full = false;
    }

    fn write(&self, state: &ProgressState, w: &mut dyn fmt::Write) {
        let eta = format!(
            "{:.1}s",
            ((state.len().unwrap_or_default().saturating_sub(state.pos())) as f64 / self.rate)
        );
        if self.force_upscale {
            let rate = self.rate * 1024.0;
            match NumberPrefix::binary(rate) {
                NumberPrefix::Standalone(number) => write!(w, "{:.0}B/s {}", number / 1024.0, eta).unwrap(),
                NumberPrefix::Prefixed(prefix, number) => {
                    write!(w, "{:.2} {}B/s", number / 1024.0, prefix).unwrap();
                }
            }
        } else {
            write!(w, "{}/s {}", BinaryBytes(self.rate as u64), eta).unwrap();
        }
    }
}

struct State {
    failed_rx: mpsc::Receiver<JobResult>,
    file_count: Arc<AtomicUsize>,
    total_size: Arc<AtomicU64>,
    complete_rx: mpsc::Receiver<JobResult>,
    copy_thread: thread::JoinHandle<()>,
    err_thread: thread::JoinHandle<()>,
}

fn work(source: PathBuf, dest: PathBuf, pb: &ProgressBar) -> State {
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
                        JobResult::ScanFailure(_) => ftx.send(scan_result).unwrap(),
                        JobResult::ScanSuccess(_) => file_set.push(scan_result),
                        JobResult::CopyFailure(_) => file_set.push(scan_result),
                        JobResult::CopySuccess(_) => ctx.send(scan_result).unwrap(),
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
                        JobResult::ScanFailure(_) => ftx.send(copy_result).unwrap(),
                        JobResult::ScanSuccess(_) => retry_set.push(copy_result),
                        JobResult::CopyFailure(_) => retry_set.push(copy_result),
                        JobResult::CopySuccess(_) => ctx.send(copy_result).unwrap(),
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
                JobResult::ScanFailure(_) => panic!(), // Shouldn't happen
                JobResult::ScanSuccess(a) => { ji.dir_entry = Some(a.dir_entry); ji.error.extend(a.error); ji.metadata = a.meta_data; true },
                JobResult::CopyFailure(a) => { ji.dir_entry = Some(a.dir_entry); ji.error.extend(a.error); ji.metadata = a.meta_data; true },
                JobResult::CopySuccess(_) => panic!(), // Shouldn't happen
                JobResult::PermaFailure(a) => { ji.dir_entry = a.dir_entry; ji.error.extend(a.error); ji.metadata = a.meta_data; false },
            };

            if can_process {
                let jobresult = copy_file(source, dest, &mut ji, pb);
                match jobresult {
                    Ok(r) => complete_tx.send(r).unwrap(),
                    Err(err) => 
                    { let error = vec![err];
                        failed_tx
                        .send(JobResult::CopyFailure(Box::new(CopyFailure {
                            error,
                            dir_entry: ji.dir_entry.unwrap(),
                            meta_data: ji.metadata,
                        })))
                        .unwrap()},
                };
            } else {
                let jobresult = JobResult::PermaFailure(Box::new(PermaFailure{
                    error: ji.error,
                    dir_entry: ji.dir_entry,
                    meta_data: ji.metadata,
                }));
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

    pb.set_message(spath.display().to_string());
    let stem = spath.strip_prefix(&source)?;
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
            fs::set_permissions(&dpath, permissions)?
        }
    } else {
        fs::create_dir_all(&dpath.parent().unwrap_or(dpath.as_path()))?;
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

    let jobresult = {
        JobResult::CopySuccess(Box::new(CopySuccess {
            error: process.error.drain(0..process.error.len()).collect::<Vec<_>>(),
            meta_data: process.metadata.take(),
        }))
    };

    Ok(jobresult)
}

// Add backoff timer to copy failures? Re-send failed dir scans back into the scanner, also with backoff timer?
enum JobResult {
    ScanFailure(Box<ScanFailure>),
    ScanSuccess(Box<ScanSuccess>),
    CopyFailure(Box<CopyFailure>),
    CopySuccess(Box<CopySuccess>),
    PermaFailure(Box<PermaFailure>),
}

struct ScanSuccess {
    error: Vec<anyhow::Error>,
    dir_entry: Box<DirEntry>,
    meta_data: Option<Box<Metadata>>
}

struct CopyFailure {
    error: Vec<anyhow::Error>,
    dir_entry: Box<DirEntry>,
    meta_data: Option<Box<Metadata>>,
}

struct PermaFailure {
    error: Vec<anyhow::Error>,
    dir_entry: Option<Box<DirEntry>>,
    meta_data: Option<Box<Metadata>>,
}

struct CopySuccess {
    error: Vec<anyhow::Error>,
    meta_data: Option<Box<Metadata>>,
}

enum ScanFailType {
    Directory(Box<PathBuf>),
    File(Box<Result<DirEntry, std::io::Error>>)
}

struct ScanFailure {
    error: Vec<anyhow::Error>,
    kind: ScanFailType,
}

struct JobInfo {
    error: Vec<anyhow::Error>,
    dir_entry: Option<Box<DirEntry>>,
    dir: Option<Box<PathBuf>>,
    metadata: Option<Box<Metadata>>,
    size: Option<u64>,
    success: bool,
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
                                    let result = JobResult::ScanSuccess(Box::new(ScanSuccess {
                                        error: Vec::new(),
                                        dir_entry: Box::new(dir_entry),
                                        meta_data: Some(Box::new(meta_data)),
                                    }));
                                    tx.send(result).unwrap();
                                }
                                Err(err) => {
                                    let error = vec![anyhow!("Failed to read metadata: {err}")];
                                    let result =
                                        JobResult::ScanSuccess(Box::new(ScanSuccess {
                                            error,
                                            dir_entry: Box::new(dir_entry),
                                            meta_data: None,
                                        }));
                                    tx.send(result).unwrap();
                                }
                            }
                        }
                    }
                    Err(ref err) => {
                        let error = vec![anyhow!("Failed to get directory entry: {err}")];
                        let kind = ScanFailType::File(Box::new(dir_entry_result));
                        let result = JobResult::ScanFailure(Box::new(ScanFailure {error, kind}));
                        tx.send(result).unwrap();
                    }
                }
            });
        }
        Err(err) => {
            let error = vec![anyhow!("Failed to read from directory: {err}")];
            let kind = ScanFailType::Directory(Box::new(src.as_ref().to_path_buf()));
            let result = JobResult::ScanFailure(Box::new(ScanFailure { error, kind }));
            tx.send(result).unwrap();
        }
    }
}
