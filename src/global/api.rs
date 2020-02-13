use super::manager::REQUEST_SENDER;
use super::types::{Request, Response};
use crossbeam::channel;

// TODO
// there is no type constraint here on what kind of responses that can be returned for a given
// response. e.g. A TableCreated response doesn't make sense when requesting access to the type map

/// Send a resource request to the global resource manager
pub fn send_request(request: Request) -> Response {
    let (response_in, response_out) = channel::unbounded();
    REQUEST_SENDER
        .send((request, response_in))
        .expect("Global resources request channel closed");
    response_out
        .recv()
        .expect("Global resources response channel closed")
}
