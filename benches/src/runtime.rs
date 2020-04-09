use tokio::runtime::{self, Runtime};

pub fn rt() -> Runtime {
    runtime::Builder::new()
        .basic_scheduler()
        .core_threads(2)
        .enable_all()
        .build()
        .unwrap()
}
