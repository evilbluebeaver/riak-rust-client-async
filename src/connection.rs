// The connection to the Riak Procol Buffers API
//
// For more information: https://docs.basho.com/riak/kv/latest/developing/api/protocol-buffers/
//

use crate::errors::{RiakErr, ServerError};
use std::io::{self, ErrorKind};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpStream, ToSocketAddrs};

// `RiakConn` represents a connection to a Riak server.
// TODO - reconnection and connection termination handling
pub struct RiakConn {
    tcp_stream: TcpStream,
    is_broken: bool,
}

impl RiakConn {
    // Constructs a new `RiakConn`.
    //
    // This will either return the newly constructed `RiakConn` or any error
    // result of the underlying `TcpStream` that may have occurred.
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> Result<RiakConn, RiakErr> {
        let tcp_stream = TcpStream::connect(addr).await?;
        tracing::debug!("connection established!");

        Ok(RiakConn {
            tcp_stream,
            is_broken: false,
        })
    }

    // Reconnect to the `SocketAddr` originally connected to.
    pub async fn reconnect(&mut self) -> Result<(), RiakErr> {
        let addr = self.tcp_stream.peer_addr()?;
        let tcp_stream = TcpStream::connect(&addr).await?;
        tracing::debug!("reconnection established!");
        self.tcp_stream = tcp_stream;
        self.is_broken = false;
        Ok(())
    }

    // Send and Receive data via the `TcpStream` in a single action
    pub async fn exchange(
        &mut self,
        send_code: u8,
        expected_recv_code: u8,
        send_data: &[u8],
    ) -> Result<Vec<u8>, RiakErr> {
        self.send(send_code, send_data).await?;
        let response = self.receive(expected_recv_code).await?;
        self.is_broken = false;
        Ok(response)
    }

    // Stream one response frame from Riak.
    //
    // If `send_data` is `Some`, this sends the initial request first.
    // If `send_data` is `None`, this reads the next streamed response chunk.
    pub(crate) async fn stream(
        &mut self,
        send_code: u8,
        expected_recv_code: u8,
        send_data: Option<&[u8]>,
    ) -> Result<Vec<u8>, RiakErr> {
        if let Some(data) = send_data {
            self.send(send_code, data).await?;
        }
        self.receive(expected_recv_code).await
    }

    pub(crate) fn mark_stream_done(&mut self) {
        self.is_broken = false;
    }

    pub fn is_broken(&self) -> bool {
        self.is_broken
    }

    // Send data over the `TcpStream`
    async fn send(&mut self, send_code: u8, send_data: &[u8]) -> Result<(), RiakErr> {
        // If the connection is already marked as broken, we should not attempt to send data.
        // As long as any request or stream starts with send, it is safe to do the check only here.
        if self.is_broken {
            return Err(RiakErr::IoError(io::Error::new(
                ErrorKind::BrokenPipe,
                "connection has unread response data; reconnect required",
            )));
        }

        // Mark the connection broken before I/O so a dropped future cannot leave us
        // in a falsely healthy state.
        self.is_broken = true;

        // The first thing sent to Riak's Protocol Buffers API is a "header" of 5 bytes.
        //
        // https://docs.basho.com/riak/kv/latest/developing/api/protocol-buffers/#protocol
        //
        // Bytes 1 through 4 inform Riak of the number of bytes being sent, and byte 5
        // is the protocol buffer code of the message being sent. Here we record the
        // number of bytes we intend to send to Riak to generate this "header".
        let mut send_header: [u8; 5] = [0u8; 5];
        let send_bytes: u32 = (send_data.len() as u32) + 1;
        send_header[..4].copy_from_slice(&send_bytes.to_be_bytes());
        send_header[4] = send_code;
        tracing::debug!("header was {:?}", send_header);

        // Send the header over the `TcpStream`, followed by the data
        self.tcp_stream.write_all(&send_header).await?;
        self.tcp_stream.write_all(send_data).await?;
        self.tcp_stream.flush().await?;
        tracing::debug!("wrote header and data successfully!");

        Ok(())
    }

    // Receive data from the `TcpStream`
    async fn receive(&mut self, expected_recv_code: u8) -> Result<Vec<u8>, RiakErr> {
        // Retrieve the header from the server
        let mut recv_header = [0u8; 4];
        self.tcp_stream.read_exact(&mut recv_header).await?;
        tracing::debug!("received response header successfully!");

        // Convert the header to a u32.
        // This number tells us how many bytes the server will be sending
        let recv_bytes = u32::from_be_bytes(recv_header);

        // Retrieve the code, the code is the first byte after the header
        let mut recv_code = [0u8; 1];
        self.tcp_stream.read_exact(&mut recv_code).await?;
        tracing::debug!("received response code {}", recv_code[0]);

        // Retrieve the protocol buffer encoded data
        let mut response = vec![0u8; (recv_bytes - 1) as usize];
        self.tcp_stream.read_exact(&mut response).await?;
        tracing::debug!("received response of size {}", (recv_bytes - 1));

        // Check for Riak errors
        if recv_code[0] != expected_recv_code {
            let err = ServerError::new(recv_code[0], &response);
            return Err(RiakErr::ServerError(err));
        }

        // If all went well, send back the response
        Ok(response)
    }
}
