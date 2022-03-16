use anyhow::{anyhow, bail, Context, Result};
use clap::Parser;
use indicatif::{FormattedDuration, HumanBytes, ProgressBar, ProgressStyle};
use rayon::iter::{IntoParallelIterator, ParallelIterator};
use rayon::Scope;
use shellexpand;
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

fn copy_recurse<U: AsRef<Path>, V: AsRef<Path>>(from: &U, to: &V) -> Result<()> {
    let start = std::time::Instant::now();

    // let options = MatchOptions {
    //     case_sensitive: false,
    //     require_literal_separator: false,
    //     require_literal_leading_dot: false,
    // };

    let src_str = from.as_ref().to_str().unwrap(); // FIXME: Handle OsStr differently whenever https://rust-lang.github.io/rfcs/2295-os-str-pattern.html is approved.
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

    let dst_str = to.as_ref().to_str().unwrap(); // FIXME: Handle OsStr differently whenever https://rust-lang.github.io/rfcs/2295-os-str-pattern.html is approved.
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

    let (failed_tx, failed_rx) = mpsc::channel();
    let file_count = Arc::new(AtomicUsize::new(0));
    let total_size = Arc::new(AtomicU64::new(0));
    let scan_finished = Arc::new(AtomicBool::new(false));
    let (scan_tx, scan_rx) = mpsc::channel();

    let scan_thread = {
        {
            let ts = total_size.clone();
            let sf = scan_finished.clone();
            let src = source.clone();
            let scan_pb = pb.clone();
            thread::spawn(move || {
                rayon::scope(|s| scan(scan_pb, &src, scan_tx, ts, s));
                sf.store(true, Ordering::Relaxed);
            })
        }
    };

    let (complete_tx, complete_rx) = mpsc::channel();
    let (error_tx, error_rx) = mpsc::channel();

    let copy_finished = Arc::new(AtomicBool::new(false));
    let copy_thread = {
        let etx = error_tx.clone();
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
                let mut scan_err_set = Vec::new();
                let mut file_set = Vec::new();
                for scan_result in scan_results {
                    if scan_result.has_error() {
                        if scan_result.dir.is_some() {
                            etx.send(scan_result).unwrap();
                        } else {
                            scan_err_set.push(scan_result)
                        }
                    } else {
                        file_set.push(scan_result);
                    }
                }
                if !file_set.is_empty() {
                    cp_fc.fetch_add(file_set.len(), Ordering::Relaxed);
                    process_files(&cp_pb, &cp_src, &cp_dst, file_set, etx.clone(), ctx.clone());
                }
                scan_err_set.into_iter().for_each(|r| etx.send(r).unwrap());
                if done {
                    break;
                }
            }
            cp_fin.store(true, Ordering::Relaxed);
        })
    };

    scan_thread.join().unwrap();
    let err_thread = {
        let ftx = failed_tx.clone();
        let ctx = complete_tx.clone();
        let err_pb = pb.clone();
        thread::spawn(move || {
            loop {
                let done = copy_finished.load(Ordering::Relaxed);
                thread::sleep(Duration::new(0, 100_000_000)); // Sleep 100ms to give the CPU a coffee break
                let copy_results = error_rx.try_iter().into_iter().collect::<Vec<_>>();
                let mut retry_set = Vec::new();
                for copy_result in copy_results {
                    if copy_result.has_error() {
                        if copy_result.dir.is_some() {
                            ftx.send(copy_result).unwrap();
                        } else {
                            retry_set.push(copy_result);
                        }
                    } else {
                        retry_set.push(copy_result);
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

    copy_thread.join().unwrap();
    let sizes = complete_rx
        .try_iter()
        .filter_map(|r| r.size)
        .collect::<Vec<_>>(); // We effectively block here.
    let copied_data = sizes.iter().sum();

    err_thread.join().unwrap();
    pb.finish_at_current_pos();

    let perm_failed = failed_rx.try_iter().into_iter().collect::<Vec<_>>();

    if !perm_failed.is_empty() {
        perm_failed
            .into_iter()
            .for_each(|f| println!("{:?}", f.error));
        println!(
            "Copied {} files of {}, {} of {} in {}",
            sizes.len(),
            file_count.load(Ordering::Relaxed),
            HumanBytes(copied_data),
            HumanBytes(total_size.load(Ordering::Relaxed)),
            FormattedDuration(start.elapsed())
        );
    } else {
        println!(
            "Copied {} files, {} in {}",
            file_count.load(Ordering::Relaxed),
            HumanBytes(total_size.load(Ordering::Relaxed)),
            FormattedDuration(start.elapsed())
        );
    }

    Ok(())
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
    files: Vec<ActionResult>,
    failed_tx: Sender<ActionResult>,
    complete_tx: Sender<ActionResult>,
) {
    files
        .into_par_iter()
        .map_with((failed_tx, complete_tx), |senders, result| {
            (senders.clone(), result)
        })
        .for_each(|f| {
            let (senders, mut scan_result) = f;
            let (failed_tx, complete_tx) = senders;

            if scan_result.file.is_some() {
                let outcome = copy_file(source, dest, &mut scan_result, &pb);
                let err = outcome.err();
                if err.is_some() {
                    failed_tx.send(scan_result).unwrap();
                } else {
                    scan_result.error = err;
                    complete_tx.send(scan_result).unwrap();
                }
            } else {
                failed_tx.send(scan_result).unwrap();
            }
        });
}

fn copy_file<U: AsRef<Path> + Sync, V: AsRef<Path> + Sync>(
    source: &U,
    dest: &V,
    copy_result: &mut ActionResult,
    pb: &ProgressBar,
) -> Result<(), anyhow::Error> {
    // Just need to borrow this for a second...
    let direntry = copy_result.file.take().unwrap();
    let spath = direntry.path();
    // And let's put it back where we found it... :)
    copy_result.file = Some(direntry);

    let s = spath.display().to_string();
    if let Some((i, _)) = s.char_indices().rev().nth(40) {
        pb.set_message(s[i..].to_owned());
    }
    let stem = spath.strip_prefix(&source)?;
    let dpath = dest.as_ref().join(stem);

    // If we don't have the size, make sure we have the metadata. If we don't have the metadata and still can't get it, leave both it and size to None.
    if copy_result.size.is_none() {
        copy_result.metadata = copy_result
            .metadata
            .take()
            .or(spath.metadata().ok())
            .and_then(|md| {
                // If we can get the metadata, set both it and the size fields.
                copy_result.size = Some(md.len());
                Some(md)
            });
    }

    if dpath.exists() {
        let d_metadata = dpath.metadata()?;
        let mut permissions = d_metadata.permissions();
        if permissions.readonly() {
            permissions.set_readonly(false);
            fs::set_permissions(&dpath, permissions)?
        }
    } else {
        fs::create_dir_all(&dpath.parent().unwrap_or_else(|| &dpath.as_path()))?;
    }

    #[cfg(target_os = "windows")]
    let transferred = win_stuff::win_copy(&spath, &dpath, pb)?;
    if copy_result.size.is_some() {
        // let size = copy_result.size.unwrap();
        // if size != transferred {
        // println!("Liar!");
        // }
    } else {
        copy_result.size = Some(transferred);
    }

    #[cfg(not(target_os = "windows"))]
    {
        fs::copy(&spath, &dpath)?;
        if copy_result.size.is_some() {
            let size = copy_result.size.unwrap();
            pb.inc(size);
        }
    }

    Ok(())
}

struct ActionResult {
    error: Option<anyhow::Error>,
    file: Option<DirEntry>,
    dir: Option<PathBuf>,
    metadata: Option<Metadata>,
    size: Option<u64>,
}

impl ActionResult {
    fn has_error(&self) -> bool {
        self.error.is_some()
    }

    fn new() -> ActionResult {
        ActionResult {
            error: None,
            file: None,
            dir: None,
            metadata: None,
            size: None,
        }
    }
}

fn scan<'a, U: AsRef<Path>>(
    pb: ProgressBar,
    src: &U,
    tx: Sender<ActionResult>,
    total_size: Arc<AtomicU64>,
    scope: &Scope<'a>,
) {
    match fs::read_dir(src) {
        Ok(dir) => {
            dir.into_iter().for_each(|dir_entry_result| {
                let mut result = ActionResult::new();
                match dir_entry_result {
                    Ok(entry) => {
                        let path = entry.path();
                        result.file = Some(entry);
                        if path.is_dir() {
                            let tx = tx.clone();
                            let pb = pb.clone();
                            let ts = total_size.clone();
                            scope.spawn(move |s| scan(pb, &path, tx, ts, s))
                        } else {
                            if let Ok(md) = path.metadata() {
                                let size = md.len();
                                pb.inc_length(size);
                                total_size.fetch_add(size, Ordering::Relaxed);
                                result.size = Some(md.len());
                                result.metadata = Some(md);
                            } else {
                                result.error = Some(anyhow!("Failed to read metadata"));
                            }
                            tx.send(result).unwrap();
                        }
                    }
                    Err(_) => {
                        result.error = Some(anyhow!("Failed to get directory entry"));
                        tx.send(result).unwrap();
                    }
                }
            });
        }
        Err(_) => {
            let mut result = ActionResult::new();
            result.error = Some(anyhow!("Failed to read from directory"));
            result.dir = Some(src.as_ref().to_path_buf());
            tx.send(result).unwrap();
        }
    }
}
