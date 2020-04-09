use tokio::runtime::{self, Runtime};

pub fn rt() -> Runtime {
    runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(10)
        .enable_all()
        .build()
        .unwrap()
}
