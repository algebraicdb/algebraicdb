use crate::state::DbmsState;
use crate::util::Timing;
use std::path::PathBuf;
use std::sync::atomic::Ordering;
use tokio::time::delay_for;

use super::snapshot;
use super::{TransactionNumber, TRANSACTION_NUMBER};

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
    let mut last_snapshotted: TransactionNumber = startup_id;
    match timing {
        Timing::Never() => {}
        Timing::Every(duration) => loop {
            delay_for(duration).await;

            // check tip of WAL, and tip of current snapshot
            let current = TRANSACTION_NUMBER.load(Ordering::Relaxed);
            eprintln!(
                "Checking snapshot {} vs current: {}",
                last_snapshotted, current
            );
            if current != last_snapshotted {
                last_snapshotted = snapshot(&data_dir, &mut dbms).await.unwrap_or_else(|err| {
                    eprintln!("Failed to write snapshot: {:?}\n", err);
                    last_snapshotted
                });
            }
        },
    }
}
