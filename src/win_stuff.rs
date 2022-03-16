use std::os::windows::prelude::OsStrExt;
use std::ffi::OsStr;
use std::io::{self, Error, ErrorKind};
use std::ffi::c_void;
use std::ptr;
use indicatif::ProgressBar;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Storage::FileSystem;
use std::path::Path;

// use windows_sys::Win32::System::WindowsProgramming::*;
// need to detect when the OS will support COPY_FILE_REQUEST_COMPRESSED_TRAFFIC

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

pub(crate) fn win_copy<U: AsRef<Path>, V: AsRef<Path>>(
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
            0 //COPY_FILE_REQUEST_COMPRESSED_TRAFFIC,
        )
    };
    if boolresult != 0 {
        // nonzero means success according to ms documents https://docs.microsoft.com/en-us/windows/win32/api/winbase/nf-winbase-copyfileexw
        Ok(last_transferred)
    } else {
        Err(Error::last_os_error())
    }
}
