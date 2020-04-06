use crate::util::{NumBytes, Timing};
use std::path::PathBuf;
use structopt::StructOpt;

#[derive(StructOpt)]
pub struct DbmsConfig {
    /// Do not write anything to disk.
    /// This option will make the dbms store all data in memory.
    /// Setting this option will make the `data_dir` option be ignored.
    #[structopt(long)]
    pub no_persistence: bool,

    /// Aftec the WAL reaches this many bytes, truncate it.
    #[structopt(long, env = "ALGDB_WAL_TRUNCATE_AT", default_value = "1G")]
    pub wal_truncate_at: NumBytes,

    /// Determine when the dbms should try to flush to disk.
    /// Has no effect if `no_persistence` is set.
    #[structopt(long, env = "ALGDB_SNAPSHOT_TIMING", default_value = "30s")]
    pub disk_flush_timing: Timing,

    /// The dbms data directory.
    /// This option will make the dbms store all data in memory.
    /// Has no effect if `no_persistence` is set.
    #[structopt(
        long,
        parse(from_os_str),
        env = "ALGDB_DATA_DIR",
        default_value = "./data"
    )]
    pub data_dir: PathBuf,
}

impl DbmsConfig {
    /// A configuration suitable for unit/integration testing
    ///
    /// This config will not write anything to disk.
    pub fn testing_config() -> Self {
        Self {
            no_persistence: true,
            ..Default::default()
        }
    }
}

impl Default for DbmsConfig {
    fn default() -> Self {
        Self {
            no_persistence: false,
            wal_truncate_at: "1G".parse().unwrap(),
            disk_flush_timing: "30s".parse().unwrap(),
            data_dir: "./data".parse().unwrap(),
        }
    }
}
