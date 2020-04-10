use tokio::runtime::{self, Runtime};

pub fn brt() -> Runtime {
    runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(10)
        .enable_all()
        .build()
        .unwrap()
}
pub fn srt() -> Runtime {
    runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(10)
        .enable_all()
        .build()
        .unwrap()
}

pub fn rt() -> Runtime {
    runtime::Builder::new()
        .threaded_scheduler()
        .core_threads(10)
        .enable_all()
        .build()
        .unwrap()
}
