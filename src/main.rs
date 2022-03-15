use clap::Parser;
use indicatif::{FormattedDuration, HumanBytes, ProgressBar, ProgressStyle};
use rayon::iter::{Either, IntoParallelIterator, ParallelIterator};
use rayon::Scope;
use std::ffi::{c_void, OsStr};
use std::fs::{self, DirEntry};
use std::io::{Error, ErrorKind};
use std::os::windows::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{self, Sender};
use std::sync::Arc;
use std::time::Duration;
use std::{io, ptr, thread};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Storage::FileSystem;
use windows_sys::Win32::System::WindowsProgramming::*;

#[derive(Debug, Parser)]
#[clap(name = "fcp", about = "Multi-threaded copy...in rust!")]
struct Opt {
    /// Source directory
    source: String,

    /// Destination directory
    dest: String,
    // / Allow copying from encrypted location to unencrypted location
    // allow_efs_to_nonefs: bool;
}

fn unrolled_find_u16s(needle: u16, haystack: &[u16]) -> Option<usize> {
    let ptr = haystack.as_ptr();
    let mut start = haystack;

    // For performance reasons unfold the loop eight times.
    while start.len() >= 8 {
        macro_rules! if_return {
            ($($n:literal,)+) => {
                $(
                    if start[$n] == needle {
                        return Some((&start[$n] as *const u16 as usize - ptr as usize) / 2);
                    }
                )+
            }
        }

        if_return!(0, 1, 2, 3, 4, 5, 6, 7,);

        start = &start[8..];
    }

    for c in start {
        if *c == needle {
            return Some((c as *const u16 as usize - ptr as usize) / 2);
        }
    }
    None
}

fn to_u16s<S: AsRef<OsStr>>(s: S) -> std::io::Result<Vec<u16>> {
    fn inner(s: &OsStr) -> crate::io::Result<Vec<u16>> {
        let mut maybe_result: Vec<u16> = s.encode_wide().collect();
        if unrolled_find_u16s(0, &maybe_result).is_some() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "strings passed to WinAPI cannot contain NULs",
            ));
        }
        maybe_result.push(0);
        Ok(maybe_result)
    }
    inner(s.as_ref())
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

    let pb = ProgressBar::new(0);
    pb.set_style(
        ProgressStyle::default_bar()
            .template(
                "{spinner:.green} [{elapsed_precise}] [{bar:50.cyan/blue}] {bytes}/{total_bytes} {bytes_per_sec} ({eta}) [{wide_msg}]",
            )
            .progress_chars("#>-")
            // .on_finish(finish)
    );

    let (failed_tx, failed_rx) = mpsc::channel();
    let (size_tx, size_rx) = mpsc::channel();
    let file_count = Arc::new(AtomicUsize::new(0));
    let total_size = AtomicU64::new(0);

    {
        let (scan_tx, scan_rx) = mpsc::channel();
        let scan_finished = Arc::new(AtomicBool::new(false));
        {
            let sf = scan_finished.clone();
            let src = source.clone();
            let scan_pb = pb.clone();
            thread::spawn(move || {
                rayon::scope(|s| scan(scan_pb, &src, scan_tx, s));
                sf.store(true, Ordering::Relaxed);
            });
        }

        let (err_tx, err_rx) = mpsc::channel();

        let copy_finished = Arc::new(AtomicBool::new(false));
        {
            let sztx = size_tx.clone();
            let cp_fin = copy_finished.clone();
            let cp_pb = pb.clone();
            let cp_fc = file_count.clone();
            let cp_src = source.clone();
            let cp_dst = dest.clone();
            thread::spawn(move || {
                loop {
                    let done = scan_finished.load(Ordering::Relaxed);
                    thread::sleep(Duration::new(0, 100_000_000)); // Sleep 100ms to give the CPU a coffee break
                    let received: (Vec<_>, Vec<_>) = scan_rx.try_recv().into_iter().unzip();
                    let (file_set, _sizes) = received;
                    if !file_set.is_empty() {
                        cp_fc.fetch_add(file_set.len(), Ordering::Relaxed);
                        let output =
                            process_files(&cp_pb, &cp_src, &cp_dst, file_set, sztx.clone());
                        err_tx.send(output).unwrap();
                    }
                    if done {
                        break;
                    }
                }
                cp_fin.store(true, Ordering::Relaxed);
            });
        }

        {
            let err_pb = pb.clone();
            thread::spawn(move || {
                loop {
                    let done = copy_finished.load(Ordering::Relaxed);
                    thread::sleep(Duration::new(0, 100_000_000)); // Sleep 100ms to give the CPU a coffee break
                    let err_out: (Vec<_>, Vec<_>) = err_rx.try_recv().into_iter().unzip();
                    let (err_set, _) = err_out;
                    if !err_set.is_empty() {
                        
                        let err_set = err_set.into_iter().flatten().collect::<Vec<_>>();
                        let perm_failed =
                            process_files(&err_pb, &source, &dest, err_set, size_tx.clone()).1;
                        failed_tx.send(perm_failed).unwrap();
                    }
                    if done {
                        break;
                    }
                }
            });
        }
    }

    let sizes = size_rx.iter().collect::<Vec<_>>(); // We effectively block here.
    let copied_data = sizes.iter().sum();

    pb.finish_at_current_pos();

    let perm_failed = failed_rx.recv().into_iter().flatten().collect::<Vec<_>>();

    if !perm_failed.is_empty() {
        perm_failed
            .into_iter()
            .map(|f| f.1)
            .for_each(|f| println!("{}", f));
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
) -> (Vec<DirEntryResult>, Vec<(DirEntryResult, String)>) {
    // Won't ever get used, but it makes the compiler happy
    let default = PathBuf::from(".");

    files
        .into_par_iter()
        .map_with(tx, |s, x| (s.clone(), x))
        .partition_map(|f: (Sender<u64>, DirEntryResult)| {
            let (tx, file) = f;
            macro_rules! handleit {
                ($dothis:expr, $err:ident, $msg:expr) => {
                    distribute_output!($dothis, file, $msg, $err)
                };
            }
            let entry = handleit!(file, err, format!("Error: {:?}", err.kind()));
            macro_rules! ioerr {
                ($step:expr, $err:expr, $err_str:expr) => {
                    format!(
                        "{}: {:?} {} for file {}",
                        $step,
                        $err,
                        $err_str,
                        entry.path().as_os_str().to_string_lossy().into_owned()
                    )
                };
            }

            let spath = entry.path();
            let s = spath.display().to_string();
            if let Some((i, _)) = s.char_indices().rev().nth(50) {
                pb.set_message(s[i..].to_owned());
            }
            let strip = spath.strip_prefix(&source);
            let stem = handleit!(strip, err, ioerr!("Strip Prefix Error", err, ""));
            let dpath = dest.as_ref().join(&stem);
            let size = handleit!(
                entry.metadata(),
                err,
                ioerr!(
                    "Metadata Fetch Error {}",
                    err.kind(),
                    err.raw_os_error().unwrap_or_default()
                )
            )
            .len();
            handleit!(
                fs::create_dir_all(&dpath.parent().unwrap_or_else(|| default.as_path())),
                err,
                ioerr!("Directory Create Error", err.kind(), "")
            );
            if dpath.exists() {
                let dmd = dpath.metadata();
                let d_metadata = handleit!(
                    dmd,
                    err,
                    ioerr!(
                        "Metadata Fetch Error",
                        err.kind(),
                        err.raw_os_error().unwrap_or_default()
                    )
                );
                let mut permissions = d_metadata.permissions();
                if permissions.readonly() {
                    permissions.set_readonly(false);
                    handleit!(
                        fs::set_permissions(&dpath, permissions),
                        err,
                        ioerr!(
                            "Error unsetting readonly",
                            err.kind(),
                            err.raw_os_error().unwrap_or_default()
                        )
                    );
                }
            }

            #[cfg(target_os = "windows")]
            let copied = *handleit!(
                win_copy(&spath, &dpath, pb),
                err,
                ioerr!(
                    "Error while copying",
                    err.kind(),
                    err.raw_os_error().unwrap_or_default()
                )
            );

            #[cfg(not(target_os = "windows"))]
            {
                handleit!(
                    fs::copy(&spath, &dpath),
                    err,
                    ioerr!(
                        "Error while copying",
                        err.kind(),
                        err.raw_os_error().unwrap_or_default()
                    )
                );
                pb.inc(size);
            }

            if copied != size {
                println!("Liar!")
            }
            tx.send(size).unwrap();
            Either::Left(file)
        })
}

fn win_copy<U: AsRef<Path>, V: AsRef<Path>>(
    spath: U,
    dpath: V,
    pb: &ProgressBar,
) -> io::Result<u64> {
    let pfrom = to_u16s(spath.as_ref()).unwrap();
    let pto = to_u16s(dpath.as_ref()).unwrap();
    #[allow(non_snake_case)]
    unsafe extern "system" fn callback(
        _TotalFileSize: i64,
        TotalBytesTransferred: i64,
        _StreamSize: i64,
        _StreamBytesTransferred: i64,
        _dwStreamNumber: u32,
        _dwCallbackReason: u32, //LPPROGRESS_ROUTINE_CALLBACK_REASON,
        _hSourceFile: HANDLE,
        _hDestinationFile: HANDLE,
        lpData: *const c_void,
    ) -> u32 {
        let p_prog_data = lpData as *mut &mut dyn FnMut(u64);
        let prog_data = &mut *p_prog_data;
        (*prog_data)(TotalBytesTransferred as _);
        0
    }
    let mut last_transferred = 0;
    let mut total_transferred = 0;
    let mut inc_pb = |just_transferred: u64| {
        pb.inc(just_transferred - last_transferred);
        last_transferred = just_transferred;
        total_transferred += just_transferred - last_transferred;
    };
    let mut func = &mut inc_pb as &mut dyn FnMut(u64);
    let boolresult = unsafe {
        // Make this into a Result<T>
        FileSystem::CopyFileExW(
            pfrom.as_ptr(),
            pto.as_ptr(),
            Some(callback),
            ptr::addr_of_mut!(func) as *mut c_void,
            ptr::null_mut(),
            COPY_FILE_REQUEST_COMPRESSED_TRAFFIC,
        )
    };
    if boolresult != 0 {
        // nonzero means success according to ms documents https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-copyfileexw
        Ok(last_transferred)
    } else {
        Err(Error::last_os_error())
    }
}

fn scan<'a, U: AsRef<Path>>(
    pb: ProgressBar,
    src: &U,
    tx: Sender<(DirEntryResult, u64)>,
    scope: &Scope<'a>,
) {
    if src.as_ref().is_dir() {
        let dir = fs::read_dir(src).unwrap();
        dir.into_iter().for_each(|entry| {
            let info = entry.as_ref().unwrap();
            let path = info.path();

            if path.is_dir() {
                let tx = tx.clone();
                let pb = pb.clone();
                scope.spawn(move |s| scan(pb, &path, tx, s))
            } else {
                // dbg!("{}", path.as_os_str().to_string_lossy());
                let size = info.metadata().unwrap().len();
                pb.inc_length(size);
                tx.send((entry, size)).unwrap();
            }
        });
    } else {
    }
}
