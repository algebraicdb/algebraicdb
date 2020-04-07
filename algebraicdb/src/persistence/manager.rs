use crate::state::DbmsState;
use crate::util::Timing;
use std::path::PathBuf;
use tokio::time::delay_for;

use super::snapshot;
use super::TransactionNumber;

pub fn spawn_snapshotter(
    dbms: DbmsState,
    data_dir: PathBuf,
    timing: Timing,
    transaction_number: TransactionNumber,
) {
    tokio::task::spawn(manager(dbms, data_dir, timing, transaction_number));
}

async fn manager(
    mut dbms: DbmsState,
    data_dir: PathBuf,
    timing: Timing,
    startup_id: TransactionNumber,
) {
    let mut wal = dbms.wal().unwrap_or_else(|| panic!("No WAL")).clone();
    let mut last_snapshotted: TransactionNumber = startup_id;
    match timing {
        Timing::Never() => {}
        Timing::Every(duration) => loop {
            delay_for(duration).await;

            // check tip of WAL, and tip of current snapshot
            let current = wal.transaction_number();
            eprintln!(
                "Checking snapshot {} vs current: {}",
                last_snapshotted, current
            );

            assert!(current >= last_snapshotted);
            if current > last_snapshotted {
                last_snapshotted = snapshot(&data_dir, &mut dbms).await.unwrap_or_else(|err| {
                    eprintln!("Failed to write snapshot: {:?}\n", err);
                    last_snapshotted
                });

                if let Err(e) = wal.truncate_wal(last_snapshotted).await {
                    eprintln!("Failed to truncate wal: {:?}", e);
                }
            }
        },
    }
}
