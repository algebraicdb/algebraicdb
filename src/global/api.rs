use super::manager::REQUEST_SENDER;
use super::types::{Request, Response};
use tokio::sync::oneshot;

// TODO
// there is no type constraint here on what kind of responses that can be returned for a given
// response. e.g. A TableCreated response doesn't make sense when requesting access to the type map

/// Send a resource request to the global resource manager
pub async fn send_request(request: Request) -> Response {
    let (response_in, response_out) = oneshot::channel();
    REQUEST_SENDER
        .send((request, response_in))
        .unwrap_or_else(|_| panic!("Global resources request channel closed"));
    response_out
        .await
        .expect("Global resources request channel closed")
}
