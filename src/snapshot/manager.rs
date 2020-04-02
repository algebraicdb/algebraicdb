
use tokio::time::{delay_for, Duration};
use crate::local::dbms_state::DbmsState;
use crate::wal::{TRANSACTION_NUMBER, TransactionNumber};
use std::sync::atomic::Ordering;

use super::snapshot;

pub fn spawn_snapshotter(dbms: DbmsState, transaction_number: TransactionNumber) {
    tokio::task::spawn(async move {
        manager(dbms, transaction_number).await
    });
}

async fn manager(mut dbms: DbmsState, startup_id: TransactionNumber) {
    let mut last_snapshotted: TransactionNumber = startup_id;
    loop {
        delay_for(Duration::new(30, 0)).await;

        // check tip of WAL, and tip of current snapshot
        let current = TRANSACTION_NUMBER.load(Ordering::Relaxed);
        eprintln!("Checking snapshot {} vs current: {}", last_snapshotted, current);
        if current != last_snapshotted {
            last_snapshotted = snapshot(&mut dbms).await.unwrap_or_else(|err| {
                eprintln!("Failed to write snapshot: {:?}\n", err);
                last_snapshotted
            });
        }
    }
}
