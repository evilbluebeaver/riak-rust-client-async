/// A list of every bucket on the cluster (Note: These functions can be expensive for the server. Do not
/// use in performance-sensitive code.)
///
/// For more information: https://docs.basho.com/riak/kv/latest/developing/api/protocol-buffers/list-buckets/
use crate::codes;
use crate::connection::RiakConn;
use crate::errors::RiakErr;
use crate::proto::{
    RpbIndexReq, RpbIndexResp, RpbListBucketsReq, RpbListBucketsResp, RpbListKeysReq,
    RpbListKeysResp,
};
use crate::secondary_index::{IndexReq, IndexResp};
use prost::Message;

/// Trait for protocol-specific streaming behavior.
trait StreamProtocol {
    type Item;
    type Request: Message;
    type Response: Message + Default;
    const REQUEST_CODE: u8;
    const RESPONSE_CODE: u8;
    /// Build the initial request. Returns `None` after the first call.
    fn build_request(&mut self) -> Option<Self::Request>;
    /// Extract the output item and `done` flag from the decoded response.
    fn get_item(response: Self::Response) -> (Self::Item, bool);
}

/// Generic streaming struct that handles the state machine.
struct RiakStream<'a, P: StreamProtocol> {
    connection: &'a mut RiakConn,
    done: bool,
    protocol: P,
}

impl<'a, P: StreamProtocol> RiakStream<'a, P> {
    fn new(connection: &'a mut RiakConn, protocol: P) -> Self {
        RiakStream {
            connection,
            done: false,
            protocol,
        }
    }

    /// Return the next item from the stream.
    async fn next(&mut self) -> Option<Result<P::Item, RiakErr>> {
        if self.done {
            return None;
        }
        Some(self.try_next().await)
    }

    async fn try_next(&mut self) -> Result<P::Item, RiakErr> {
        let request = self.protocol.build_request().map(|r| r.encode_to_vec());
        let raw = self
            .connection
            .stream(P::REQUEST_CODE, P::RESPONSE_CODE, request.as_deref())
            .await?;

        let decoded = P::Response::decode(raw.as_slice())?;
        let (item, done) = P::get_item(decoded);
        self.done = done;
        if done {
            self.connection.mark_stream_done();
        }
        Ok(item)
    }

    /// Collect all items from the stream.
    async fn all(&mut self) -> Result<Vec<P::Item>, RiakErr> {
        let mut items = Vec::new();
        while let Some(result) = self.next().await {
            items.push(result?);
        }
        Ok(items)
    }
}

// --- BucketStream ---

struct BucketProtocol(Option<()>);

impl StreamProtocol for BucketProtocol {
    type Item = Vec<Vec<u8>>;
    type Request = RpbListBucketsReq;
    type Response = RpbListBucketsResp;
    const REQUEST_CODE: u8 = codes::RpbListBucketsReq;
    const RESPONSE_CODE: u8 = codes::RpbListBucketsResp;

    fn build_request(&mut self) -> Option<RpbListBucketsReq> {
        self.0.take()?;
        Some(RpbListBucketsReq {
            stream: Some(true),
            ..Default::default()
        })
    }

    fn get_item(response: RpbListBucketsResp) -> (Vec<Vec<u8>>, bool) {
        (response.buckets, response.done.unwrap_or_default())
    }
}

/// `BucketStream` represents a list of bucket names in Riak
pub struct BucketStream<'a>(RiakStream<'a, BucketProtocol>);

impl<'a> BucketStream<'a> {
    /// constructs a new `BucketStream`
    pub async fn new(connection: &'a mut RiakConn) -> Result<BucketStream<'a>, RiakErr> {
        Ok(BucketStream(RiakStream::new(
            connection,
            BucketProtocol(Some(())),
        )))
    }

    /// return a list of every bucket from the stream
    pub async fn all(&mut self) -> Result<Vec<Vec<u8>>, RiakErr> {
        let batches = self.0.all().await?;
        Ok(batches.into_iter().flatten().collect())
    }

    /// return the next group of buckets from the stream
    pub async fn next(&mut self) -> Option<Result<Vec<Vec<u8>>, RiakErr>> {
        self.0.next().await
    }
}

// --- KeyStream ---

struct KeyProtocol {
    bucket: Option<Vec<u8>>,
}

impl StreamProtocol for KeyProtocol {
    type Item = Vec<Vec<u8>>;
    type Request = RpbListKeysReq;
    type Response = RpbListKeysResp;
    const REQUEST_CODE: u8 = codes::RpbListKeysReq;
    const RESPONSE_CODE: u8 = codes::RpbListKeysResp;

    fn build_request(&mut self) -> Option<RpbListKeysReq> {
        let bucket = self.bucket.take()?;
        Some(RpbListKeysReq {
            bucket,
            ..Default::default()
        })
    }

    fn get_item(response: RpbListKeysResp) -> (Vec<Vec<u8>>, bool) {
        (response.keys, response.done.unwrap_or_default())
    }
}

/// `KeyStream` represents a list of keys in a Riak bucket
pub struct KeyStream<'a>(RiakStream<'a, KeyProtocol>);

impl<'a> KeyStream<'a> {
    /// constructs a new `KeyStream`
    pub async fn new(
        connection: &'a mut RiakConn,
        bucket: Vec<u8>,
    ) -> Result<KeyStream<'a>, RiakErr> {
        Ok(KeyStream(RiakStream::new(
            connection,
            KeyProtocol {
                bucket: Some(bucket),
            },
        )))
    }

    /// return a list of all the keys from the stream
    pub async fn all(&mut self) -> Result<Vec<Vec<u8>>, RiakErr> {
        let batches = self.0.all().await?;
        Ok(batches.into_iter().flatten().collect())
    }

    /// return the next group of keys from the stream
    pub async fn next(&mut self) -> Option<Result<Vec<Vec<u8>>, RiakErr>> {
        self.0.next().await
    }
}

// --- SecondaryIndexStream ---

struct IndexProtocol {
    request: Option<RpbIndexReq>,
}

impl StreamProtocol for IndexProtocol {
    type Item = IndexResp;
    type Request = RpbIndexReq;
    type Response = RpbIndexResp;
    const REQUEST_CODE: u8 = codes::RpbIndexReq;
    const RESPONSE_CODE: u8 = codes::RpbIndexResp;

    fn build_request(&mut self) -> Option<RpbIndexReq> {
        self.request.take()
    }

    fn get_item(response: RpbIndexResp) -> (IndexResp, bool) {
        let done = response.done.unwrap_or_default();
        (IndexResp::from(response), done)
    }
}

/// `SecondaryIndexStream` represents a streaming 2i search
pub struct SecondaryIndexStream<'a>(RiakStream<'a, IndexProtocol>);

impl<'a> SecondaryIndexStream<'a> {
    /// constructs a new `SecondaryIndexStream`
    pub async fn new(
        connection: &'a mut RiakConn,
        request: IndexReq,
    ) -> Result<SecondaryIndexStream<'a>, RiakErr> {
        Ok(SecondaryIndexStream(RiakStream::new(
            connection,
            IndexProtocol {
                request: Some(request.0),
            },
        )))
    }

    /// retrieves all the IndexResp for the IndexReq
    pub async fn all(&mut self) -> Result<Vec<IndexResp>, RiakErr> {
        self.0.all().await
    }

    /// retrieves the next IndexResp
    pub async fn next(&mut self) -> Option<Result<IndexResp, RiakErr>> {
        self.0.next().await
    }
}
