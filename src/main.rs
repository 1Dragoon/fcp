use clap::Parser;
use indicatif::{FormattedDuration, HumanBytes, ProgressBar, ProgressStyle};
use rayon::iter::{Either, IntoParallelIterator, ParallelIterator};
use rayon::Scope;
use std::ffi::{c_void, OsStr};
use std::fs::{self};
use std::io::{Error, ErrorKind};
use std::os::windows::prelude::OsStrExt;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender};
use std::{io, ptr};
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Storage::FileSystem;

#[derive(Debug, Parser)]
#[clap(name = "fcp", about = "Multi-threaded copy...in rust!")]
struct Opt {
    /// Source directory
    source: String,

    /// Destination directory
    dest: String,
}

// type LPPROGRESS_ROUTINE_CALLBACK_REASON = u32;
// type LPPROGRESS_ROUTINE = Option<
//     unsafe extern "system" fn(
//         totalfilesize: i64,
//         totalbytestransferred: i64,
//         streamsize: i64,
//         streambytestransferred: i64,
//         dwstreamnumber: u32,
//         dwcallbackreason: LPPROGRESS_ROUTINE_CALLBACK_REASON,
//         hsourcefile: HANDLE,
//         hdestinationfile: HANDLE,
//         lpdata: *const c_void,
//     ) -> u32,
// >;

// struct ProgStruct {
//     progressbar: ProgressBar
// }

// trait IncProg {
//     fn incProg(&self, inc_by: u64);
// }

// impl IncProg for ProgStruct {

// }

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

// #[derive(Debug)]
// struct Custom {
//     kind: ErrorKind,
//     error: Box<dyn error::Error + Send + Sync>,
// }

// enum Repr {
//     Os(i32),
//     Simple(ErrorKind),
//     // &str is a fat pointer, but &&str is a thin pointer.
//     SimpleMessage(ErrorKind, &'static &'static str),
//     Custom(Box<Custom>),
// }

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

// #[allow(non_snake_case)]
// unsafe extern "system" fn callback(
//     _TotalFileSize: i64,
//     _TotalBytesTransferred: i64,
//     _StreamSize: i64,
//     StreamBytesTransferred: i64,
//     dwStreamNumber: u32,
//     _dwCallbackReason: u32,
//     _hSourceFile: HANDLE,
//     _hDestinationFile: HANDLE,
//     lpData: *const c_void,
// ) -> u32 {
//     *(lpData as *mut i64) = StreamBytesTransferred;
//     0
// }

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
    if !source.is_dir() {
        let size = source.metadata().unwrap().len();
        tx.send((source.clone(), size)).unwrap();
    } else {
        rayon::scope(|s| scan(&source, tx, s));
    }
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

struct ProgressData<'a> {
    func: &'a mut dyn FnMut(&mut u64, u64),
    prev_total: &'a mut u64,
}

fn process_files<U: AsRef<Path> + Sync, V: AsRef<Path> + Sync>(
    pb: &ProgressBar,
    source: &U,
    dest: &V,
    files: Vec<PathBuf>,
    tx: Sender<u64>,
) -> (Vec<PathBuf>, Vec<(PathBuf, String)>) {
    // Won't ever get used, but it makes the compiler happy
    let default = PathBuf::from(".");

    files
        .into_par_iter()
        .map_with(tx, |s, x| (s.clone(), x))
        .partition_map(|f: (Sender<u64>, PathBuf)| {
            let (tx, spath) = f;
            macro_rules! handleit {
                ($dothis:expr, $err:ident, $msg:expr) => {
                    distribute_output!($dothis, spath, $msg, $err)
                };
            }
            // let entry = handleit!(spath, err, format!("Error: {:?}", err.kind()));
            macro_rules! ioerr {
                ($step:expr, $err:expr) => {
                    format!(
                        "{}: {:?} for file {}",
                        $step,
                        $err,
                        spath.as_os_str().to_string_lossy().into_owned()
                    )
                };
            }

            // let spath = file.as_os_str().to_string_lossy().into_owned();
            pb.set_message(format!("{}", spath.display()));
            let strip = spath.strip_prefix(&source);
            let stem = handleit!(strip, err, ioerr!("Strip Prefix Error", err));
            let dpath = dest.as_ref().join(&stem);
            let size = handleit!(
                fs::metadata(&spath),
                err,
                ioerr!("Metadata Fetch Error {}", err.kind())
            )
            .len();
            handleit!(
                fs::create_dir_all(&dpath.parent().unwrap_or_else(|| default.as_path())),
                err,
                ioerr!("Directory Create Error", err.kind())
            );
            if dpath.exists() {
                let dmd = dpath.metadata();
                let d_metadata = handleit!(dmd, err, ioerr!("Metadata Fetch Error", err.kind()));
                let mut permissions = d_metadata.permissions();
                if permissions.readonly() {
                    permissions.set_readonly(false);
                    handleit!(
                        fs::set_permissions(&dpath, permissions),
                        err,
                        ioerr!("Error unsetting readonly", err.kind())
                    );
                }
            }

            // handleit!(
            //     win_copy(&spath, &dpath, pb),
            //     err,
            //     ioerr!("Error while copying", err.kind())
            // );

            handleit!(
                fs::copy(&spath, &dpath),
                err,
                ioerr!("Error while copying", err.kind())
            );
            tx.send(size).unwrap();
            pb.inc(size);
            Either::Left(spath)
        })
}

fn win_copy<U: AsRef<Path>, V: AsRef<Path>>(
    spath: U,
    dpath: V,
    pb: &ProgressBar,
) -> io::Result<()> {
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
        let p_prog_data = lpData as *mut ProgressData;
        let prog_data = &mut *p_prog_data;
        (*prog_data.func)(prog_data.prev_total, TotalBytesTransferred as _);
        0
    }
    let mut prev_total = 0u64;
    let mut inc_pb = |prev_transferred: &mut u64, just_transferred: u64| {
        pb.inc(just_transferred - *prev_transferred);
        *prev_transferred = just_transferred;
    };
    let mut prog_data = ProgressData {
        func: &mut inc_pb,
        prev_total: &mut prev_total,
    };
    let err = unsafe {
        // Make this into a Result<T>
        FileSystem::CopyFileExW(
            pfrom.as_ptr(),
            pto.as_ptr(),
            Some(callback),
            &mut prog_data as *mut _ as *mut c_void,
            ptr::null_mut(),
            0,
        )
    };
    if err == 0 {
        Ok(())
    } else {
        Err(Error::new(
            ErrorKind::InvalidInput,
            format!("HRESULT: {}", err),
        ))
    }

    // #[allow(non_snake_case)]
    // unsafe extern "system" fn callback(
    //     _TotalFileSize: i64,
    //     TotalBytesTransferred: i64,
    //     _StreamSize: i64,
    //     _StreamBytesTransferred: i64,
    //     _dwStreamNumber: u32,
    //     _dwCallbackReason: LPPROGRESS_ROUTINE_CALLBACK_REASON,
    //     _hSourceFile: HANDLE,
    //     _hDestinationFile: HANDLE,
    //     lpData: *const c_void,
    // ) -> u32 {
    //     let tuple = lpData as *mut (&mut u64, &mut dyn FnMut(&mut u64, u64));
    //     let (prev_transferred, func) = &mut *tuple;
    //     (*func)(prev_transferred, TotalBytesTransferred as _);
    //     0
    // }

    // let mut last_transferred = 0u64;

    // let mut inc_pb = |prev_transferred: &mut u64, just_transferred: u64| {
    //     pb.inc(just_transferred - *prev_transferred);
    //     *prev_transferred = just_transferred;
    // };

    // let mut tuple = (&mut last_transferred, &mut inc_pb as &mut dyn FnMut(&mut u64, u64));

    // unsafe {
    //     FileSystem::CopyFileExW(
    //         pfrom.as_ptr(),
    //         pto.as_ptr(),
    //         Some(callback),
    //         &mut tuple as *mut (&mut u64, &mut dyn FnMut(&mut u64, u64)) as *mut c_void,
    //         ptr::null_mut(),
    //         0,
    //     )
    // };

    // unsafe {FileSystem::CopyFileExW(pfrom.as_ptr(), pto.as_ptr(), Some(callback), &mut func as *mut (&mut u64, &mut dyn FnMut(&mut u64, u64)) as *mut c_void, &mut 0, 0); }
}

fn scan<'a, U: AsRef<Path>>(src: &U, tx: Sender<(PathBuf, u64)>, scope: &Scope<'a>) {
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
            tx.send((path, size)).unwrap();
        }
    });
}
