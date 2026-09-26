//! Process-level plumbing: the CPU-feature guard, the priority class, and the
//! commit-charge numbers month mode budgets against (commit, not RAM, is the
//! ceiling on these machines -- see CLAUDE.md).

/// The build targets x86-64-v3. Check the features it assumes before anything
/// else runs, so an older CPU gets a message instead of
/// STATUS_ILLEGAL_INSTRUCTION.
pub fn missing_cpu_features() -> Vec<&'static str> {
    #[cfg(target_arch = "x86_64")]
    {
        let have = [
            ("avx2", std::arch::is_x86_feature_detected!("avx2")),
            ("bmi2", std::arch::is_x86_feature_detected!("bmi2")),
            ("fma", std::arch::is_x86_feature_detected!("fma")),
            ("lzcnt", std::arch::is_x86_feature_detected!("lzcnt")),
            ("movbe", std::arch::is_x86_feature_detected!("movbe")),
        ];
        have.iter().filter(|(_, ok)| !ok).map(|(n, _)| *n).collect()
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        Vec::new()
    }
}

/// The target features this binary was compiled with.
pub fn compiled_features() -> String {
    let mut v = Vec::new();
    if cfg!(target_feature = "avx2") {
        v.push("avx2");
    }
    if cfg!(target_feature = "bmi2") {
        v.push("bmi2");
    }
    if cfg!(target_feature = "fma") {
        v.push("fma");
    }
    if cfg!(target_feature = "lzcnt") {
        v.push("lzcnt");
    }
    if cfg!(target_feature = "movbe") {
        v.push("movbe");
    }
    if cfg!(target_feature = "crt-static") {
        v.push("crt-static");
    }
    v.join(",")
}

#[cfg(windows)]
mod win {
    #[repr(C)]
    pub struct MemoryStatusEx {
        pub length: u32,
        pub memory_load: u32,
        pub total_phys: u64,
        pub avail_phys: u64,
        pub total_page_file: u64,
        pub avail_page_file: u64,
        pub total_virtual: u64,
        pub avail_virtual: u64,
        pub avail_extended_virtual: u64,
    }

    #[repr(C)]
    pub struct ProcessMemoryCounters {
        pub cb: u32,
        pub page_fault_count: u32,
        pub peak_working_set_size: usize,
        pub working_set_size: usize,
        pub quota_peak_paged_pool_usage: usize,
        pub quota_paged_pool_usage: usize,
        pub quota_peak_non_paged_pool_usage: usize,
        pub quota_non_paged_pool_usage: usize,
        pub pagefile_usage: usize,
        pub peak_pagefile_usage: usize,
    }

    #[link(name = "kernel32")]
    extern "system" {
        pub fn GetCurrentProcess() -> isize;
        pub fn SetPriorityClass(process: isize, class: u32) -> i32;
        pub fn GlobalMemoryStatusEx(buf: *mut MemoryStatusEx) -> i32;
        pub fn K32GetProcessMemoryInfo(
            process: isize,
            counters: *mut ProcessMemoryCounters,
            cb: u32,
        ) -> i32;
    }

    pub const BELOW_NORMAL_PRIORITY_CLASS: u32 = 0x4000;
}

/// Lower this process (and every thread in it) to BELOW_NORMAL.
pub fn set_below_normal() -> Result<(), String> {
    #[cfg(windows)]
    unsafe {
        if win::SetPriorityClass(win::GetCurrentProcess(), win::BELOW_NORMAL_PRIORITY_CLASS) == 0 {
            return Err(format!("SetPriorityClass failed: {}", std::io::Error::last_os_error()));
        }
    }
    Ok(())
}

/// (commit limit, commit available) in bytes: GlobalMemoryStatusEx's
/// ullTotalPageFile and ullAvailPageFile.
pub fn commit() -> Option<(u64, u64)> {
    #[cfg(windows)]
    unsafe {
        let mut m: win::MemoryStatusEx = std::mem::zeroed();
        m.length = std::mem::size_of::<win::MemoryStatusEx>() as u32;
        if win::GlobalMemoryStatusEx(&mut m) != 0 {
            return Some((m.total_page_file, m.avail_page_file));
        }
    }
    None
}

/// This process's peak private commit (PeakPagefileUsage).
pub fn peak_commit() -> Option<u64> {
    #[cfg(windows)]
    unsafe {
        let mut c: win::ProcessMemoryCounters = std::mem::zeroed();
        c.cb = std::mem::size_of::<win::ProcessMemoryCounters>() as u32;
        if win::K32GetProcessMemoryInfo(win::GetCurrentProcess(), &mut c, c.cb) != 0 {
            return Some(c.peak_pagefile_usage as u64);
        }
    }
    None
}

pub const VERSION: &str = env!("CARGO_PKG_VERSION");
pub const GIT_COMMIT: &str = env!("EE_GIT_COMMIT");
pub const BUILD_DATE: &str = env!("EE_BUILD_DATE");

pub fn version_line() -> String {
    format!(
        "explorer-extract {VERSION} (commit {GIT_COMMIT}, built {BUILD_DATE}, target features {})",
        compiled_features()
    )
}
