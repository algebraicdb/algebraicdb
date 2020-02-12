use super::manager::REQUEST_SENDER;
use super::types::{Request, Response};
use crossbeam::channel;

pub fn send_request(request: Request) -> Response {
    let (response_in, response_out) = channel::unbounded();
    REQUEST_SENDER
        .send((request, response_in))
        .expect("Global resources request channel closed");
    response_out
        .recv()
        .expect("Global resources response channel closed")
}
