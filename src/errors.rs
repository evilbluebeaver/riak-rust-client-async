use std::io;

/// Represents an error that has occurred on the server side.
#[derive(Debug, thiserror::Error)]
#[error("received code {code}, error was: {data}")]
pub struct ServerError {
    code: u8,
    data: String,
}

impl ServerError {
    pub fn new(error_code: u8, error_data: &[u8]) -> ServerError {
        ServerError {
            code: error_code,
            data: String::from_utf8_lossy(error_data).into_owned(),
        }
    }
}

/// Represents errors that can occur with this Riak client and its components.
#[derive(Debug, thiserror::Error)]
pub enum RiakErr {
    #[error("error pinging riak: {0}")]
    IoError(#[from] io::Error),
    #[error("connection to riak terminated: {0}")]
    ProtobufError(#[from] prost::DecodeError),
    #[error("error from server: {0}")]
    ServerError(#[from] ServerError),
}
