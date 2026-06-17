//! A Riak client for Rust.
//!
//! This client can be used to communicate with Riak clusters to send and receive objects
//! and other information. Operations are done through the `Client` struct and there are
//! several other structs designed to build data structures for sending and receiving
//! data from a Riak cluster.
//!
//! This client uses Riak's Protocol Buffers API.
//!
//! See the Protocol Buffers API documentation for more info: https://docs.basho.com/riak/kv/latest/developing/api/protocol-buffers/

// public modules
pub mod bucket;
pub mod data_type;
pub mod errors;
pub mod object;
pub mod pool;
pub mod preflist;
pub mod secondary_index;
pub mod stream;
pub mod yokozuna;

// private modules
mod codes;
mod connection;

// Generated Protobuf (prost)
#[allow(dead_code)]
mod proto {
    include!(concat!(env!("OUT_DIR"), "/_.rs"));
}

use crate::bucket::{BucketProps, BucketTypeProps};
use crate::connection::RiakConn;
use crate::data_type::{DataTypeFetchReq, DataTypeFetchResp};
use crate::errors::RiakErr;
use crate::object::{DeleteObjectReq, FetchObjectReq, FetchObjectResp, StoreObjectReq};
use crate::preflist::PreflistItem;
use crate::proto::{
    DtFetchResp, RpbGetBucketKeyPreflistReq, RpbGetBucketKeyPreflistResp, RpbGetBucketReq,
    RpbGetBucketResp, RpbGetBucketTypeReq, RpbGetResp, RpbGetServerInfoResp, RpbIndexResp,
    RpbMapRedReq, RpbMapRedResp, RpbResetBucketReq, RpbSearchQueryResp, RpbYokozunaIndexDeleteReq,
    RpbYokozunaIndexGetReq, RpbYokozunaIndexGetResp, RpbYokozunaSchema, RpbYokozunaSchemaGetReq,
    RpbYokozunaSchemaGetResp, RpbYokozunaSchemaPutReq,
};
use crate::secondary_index::{IndexReq, IndexResp};
use crate::stream::{BucketStream, KeyStream, SecondaryIndexStream};
use crate::yokozuna::{SearchQuery, SearchQueryResp, YokozunaIndex};
use prost::Message;
use tokio::net::ToSocketAddrs;

pub use crate::pool::{ClientManager, ClientPool, PooledClient};

/// `Client` Represents a connection to a Riak server's Protocol Buffers API.
pub struct Client {
    connection: RiakConn,
}

impl Client {
    /// Constructs a new `Client` with the timeout for requests with a default timeout.
    pub async fn new<A: ToSocketAddrs>(addr: A) -> Result<Client, RiakErr> {
        let connection = RiakConn::connect(addr).await?;
        Ok(Client { connection })
    }

    /// Reconnect to the Riak server originally connected to when this client was initiated.
    pub async fn reconnect(&mut self) -> Result<(), RiakErr> {
        self.connection.reconnect().await
    }

    /// Returns true when the underlying connection has unread response data.
    pub fn is_connection_broken(&self) -> bool {
        self.connection.is_broken()
    }

    /// Sends a ping message to Riak and returns a Result.
    pub async fn ping(&mut self) -> Result<(), RiakErr> {
        self.connection
            .exchange(codes::RpbPingReq, codes::RpbPingResp, &[])
            .await?;
        Ok(())
    }

    /// Get the node name and server version of the Riak server reached.
    pub async fn server_info(&mut self) -> Result<(String, String), RiakErr> {
        let response = self
            .connection
            .exchange(codes::RpbGetServerInfoReq, codes::RpbGetServerInfoResp, &[])
            .await?;

        let RpbGetServerInfoResp {
            node,
            server_version,
        } = RpbGetServerInfoResp::decode(response.as_slice())?;
        let node_name = String::from_utf8_lossy(&node.unwrap_or_default()).into_owned();
        let server_version =
            String::from_utf8_lossy(&server_version.unwrap_or_default()).into_owned();

        Ok((node_name, server_version))
    }

    /// Produces a stream of bucket names.
    ///
    /// Caution: This call can be expensive for the server. Do not use in performance-sensitive code.
    pub async fn stream_buckets(&mut self) -> Result<BucketStream<'_>, RiakErr> {
        BucketStream::new(&mut self.connection).await
    }

    /// Produces a list of bucket names.
    ///
    /// Caution: This call can be expensive for the server. Do not use in performance-sensitive code.
    pub async fn list_buckets(&mut self) -> Result<Vec<Vec<u8>>, RiakErr> {
        let mut bucket_stream = self.stream_buckets().await?;
        bucket_stream.all().await
    }

    /// Sets the properties for a bucket given a bucket name.
    pub async fn set_bucket_properties(
        &mut self,
        bucket_props: BucketProps,
    ) -> Result<(), RiakErr> {
        let bytes = bucket_props.serialize()?;

        self.connection
            .exchange(codes::RpbSetBucketReq, codes::RpbSetBucketResp, &bytes)
            .await?;
        Ok(())
    }

    /// Retrieves bucket properties for a bucket given a bucket name.
    pub async fn get_bucket_properties<T: Into<Vec<u8>>>(
        &mut self,
        bucket_name: T,
    ) -> Result<BucketProps, RiakErr> {
        let mut req = RpbGetBucketReq {
            bucket: bucket_name.into(),
            ..Default::default()
        };

        let bytes = req.encode_to_vec();

        let response = self
            .connection
            .exchange(codes::RpbGetBucketReq, codes::RpbGetBucketResp, &bytes)
            .await?;

        let RpbGetBucketResp { props } = RpbGetBucketResp::decode(response.as_slice())?;

        let bucket_name = std::mem::take(&mut req.bucket);
        let bucket_props = BucketProps::new_with_props(bucket_name, props);

        Ok(bucket_props)
    }

    /// Assigns a set of bucket properties to a bucket type.
    pub async fn set_bucket_type_properties(
        &mut self,
        bucket_type_props: BucketTypeProps,
    ) -> Result<(), RiakErr> {
        let bytes = bucket_type_props.serialize()?;

        self.connection
            .exchange(codes::RpbSetBucketTypeReq, codes::RpbSetBucketResp, &bytes)
            .await?;
        Ok(())
    }

    /// Gets the bucket properties associated with a bucket type.
    pub async fn get_bucket_type_properties<T: Into<Vec<u8>>>(
        &mut self,
        bucket_type_name: T,
    ) -> Result<BucketTypeProps, RiakErr> {
        let mut req = RpbGetBucketTypeReq {
            r#type: bucket_type_name.into(),
        };

        let bytes = req.encode_to_vec();

        let response = self
            .connection
            .exchange(codes::RpbGetBucketTypeReq, codes::RpbGetBucketResp, &bytes)
            .await?;

        let RpbGetBucketResp { props } = RpbGetBucketResp::decode(response.as_slice())?;

        let bucket_type_name = std::mem::take(&mut req.r#type);
        let bucket_type_props = BucketTypeProps::new_with_props(bucket_type_name, props);
        Ok(bucket_type_props)
    }

    /// Resets the properties for a bucket
    pub async fn reset_bucket<T: Into<Vec<u8>>>(
        &mut self,
        bucket_type_name: T,
        bucket_name: T,
    ) -> Result<(), RiakErr> {
        let request = RpbResetBucketReq {
            bucket: bucket_name.into(),
            r#type: Some(bucket_type_name.into()),
        };

        let bytes = request.encode_to_vec();

        self.connection
            .exchange(codes::RpbResetBucketReq, codes::RpbResetBucketResp, &bytes)
            .await?;
        Ok(())
    }

    /// Produces a stream of keys from a bucket given a bucket name.
    ///
    /// Note: This operation requires traversing all keys stored in the cluster and should not be used in production.
    pub async fn stream_keys<T: Into<Vec<u8>>>(
        &mut self,
        bucket: T,
    ) -> Result<KeyStream<'_>, RiakErr> {
        KeyStream::new(&mut self.connection, bucket.into()).await
    }

    /// Produces a list of keys provided a bucket name
    ///
    /// Note: This operation requires traversing all keys stored in the cluster and should not be used in production.
    pub async fn list_keys<T: Into<Vec<u8>>>(
        &mut self,
        bucket: T,
    ) -> Result<Vec<Vec<u8>>, RiakErr> {
        let mut keys = KeyStream::new(&mut self.connection, bucket.into()).await?;
        keys.all().await
    }

    /// Stores an object on the Riak server.
    pub async fn store_object(&mut self, req: StoreObjectReq) -> Result<(), RiakErr> {
        let bytes = req.serialize()?;

        self.connection
            .exchange(codes::RpbPutReq, codes::RpbPutResp, &bytes)
            .await?;
        Ok(())
    }

    /// Fetches an object from the Riak server.
    pub async fn fetch_object(&mut self, req: FetchObjectReq) -> Result<FetchObjectResp, RiakErr> {
        let bytes = req.serialize()?;

        let response = self
            .connection
            .exchange(codes::RpbGetReq, codes::RpbGetResp, &bytes)
            .await?;

        let rpb_get_resp = RpbGetResp::decode(response.as_slice())?;
        Ok(rpb_get_resp.into())
    }

    /// Deletes an object from Riak
    pub async fn delete_object(&mut self, request: DeleteObjectReq) -> Result<(), RiakErr> {
        let bytes = request.serialize()?;
        self.connection
            .exchange(codes::RpbDelReq, codes::RpbDelResp, &bytes)
            .await?;
        Ok(())
    }

    /// Fetch the preflist for a bucket/key combination.
    pub async fn fetch_preflist<T: Into<Vec<u8>>>(
        &mut self,
        bucket: T,
        key: T,
    ) -> Result<Vec<PreflistItem>, RiakErr> {
        let req = RpbGetBucketKeyPreflistReq {
            bucket: bucket.into(),
            key: key.into(),
            ..Default::default()
        };

        let bytes = req.encode_to_vec();

        let response = self
            .connection
            .exchange(
                codes::RpbGetBucketKeyPreflistReq,
                codes::RpbGetBucketKeyPreflistResp,
                &bytes,
            )
            .await?;

        let RpbGetBucketKeyPreflistResp { preflist } =
            RpbGetBucketKeyPreflistResp::decode(response.as_slice())?;

        let preflist = preflist.into_iter().map(Into::into).collect();

        Ok(preflist)
    }

    /// Create a search schema
    pub async fn set_yokozuna_schema<T: Into<Vec<u8>>>(
        &mut self,
        name: T,
        content: T,
    ) -> Result<(), RiakErr> {
        let schema = RpbYokozunaSchema {
            name: name.into(),
            content: Some(content.into()),
        };

        let req = RpbYokozunaSchemaPutReq { schema };

        let bytes = req.encode_to_vec();

        self.connection
            .exchange(codes::RpbYokozunaSchemaPutReq, codes::RpbPutResp, &bytes)
            .await?;
        Ok(())
    }

    /// Retrieve a search schema
    pub async fn get_yokozuna_schema<T: Into<Vec<u8>>>(
        &mut self,
        name: T,
    ) -> Result<Vec<u8>, RiakErr> {
        let req = RpbYokozunaSchemaGetReq { name: name.into() };

        let bytes = req.encode_to_vec();

        let response = self
            .connection
            .exchange(
                codes::RpbYokozunaSchemaGetReq,
                codes::RpbYokozunaSchemaGetResp,
                &bytes,
            )
            .await?;

        let RpbYokozunaSchemaGetResp { schema } =
            RpbYokozunaSchemaGetResp::decode(response.as_slice())?;
        let RpbYokozunaSchema { content, .. } = schema;

        Ok(content.unwrap_or_default())
    }

    /// set a search index
    pub async fn set_yokozuna_index(&mut self, index: YokozunaIndex) -> Result<(), RiakErr> {
        let bytes = index.serialize()?;

        self.connection
            .exchange(codes::RpbYokozunaIndexPutReq, codes::RpbPutResp, &bytes)
            .await?;
        Ok(())
    }

    /// get a search index
    pub async fn get_yokozuna_index<T: Into<Vec<u8>>>(
        &mut self,
        name: T,
    ) -> Result<Vec<YokozunaIndex>, RiakErr> {
        let req = RpbYokozunaIndexGetReq {
            name: Some(name.into()),
        };

        let bytes = req.encode_to_vec();

        let response = self
            .connection
            .exchange(
                codes::RpbYokozunaIndexGetReq,
                codes::RpbYokozunaIndexGetResp,
                &bytes,
            )
            .await?;

        let RpbYokozunaIndexGetResp { index } =
            RpbYokozunaIndexGetResp::decode(response.as_slice())?;

        let index = index.into_iter().map(Into::into).collect();
        Ok(index)
    }

    /// Deletes an index
    pub async fn delete_yokozuna_index<T: Into<Vec<u8>>>(
        &mut self,
        name: T,
    ) -> Result<(), RiakErr> {
        let req = RpbYokozunaIndexDeleteReq { name: name.into() };

        let bytes = req.encode_to_vec();

        self.connection
            .exchange(codes::RpbYokozunaIndexDeleteReq, codes::RpbDelResp, &bytes)
            .await?;
        Ok(())
    }

    /// Perform a Riak Search
    pub async fn search(&mut self, query: SearchQuery) -> Result<SearchQueryResp, RiakErr> {
        let bytes = query.serialize()?;

        let response = self
            .connection
            .exchange(codes::RpbSearchQueryReq, codes::RpbSearchQueryResp, &bytes)
            .await?;

        let rpb_search_query_resp = RpbSearchQueryResp::decode(response.as_slice())?;
        Ok(rpb_search_query_resp.into())
    }

    /// Perform a MapReduce Job
    pub async fn mapreduce<T: Into<Vec<u8>>>(
        &mut self,
        request: T,
        content_type: T,
    ) -> Result<Vec<Vec<u8>>, RiakErr> {
        let req = RpbMapRedReq {
            request: request.into(),
            content_type: content_type.into(),
        };

        let bytes = req.encode_to_vec();

        let mut data: Vec<Vec<u8>> = Vec::new();
        let mut is_done = false;
        let mut send_data: Option<&[u8]> = Some(&bytes);
        while !is_done {
            let response = self
                .connection
                .stream(codes::RpbMapRedReq, codes::RpbMapRedResp, send_data)
                .await?;
            send_data = None;
            let RpbMapRedResp { response, done, .. } = RpbMapRedResp::decode(response.as_slice())?;
            data.push(response.unwrap_or_default());
            is_done = done.unwrap_or_default();
        }
        self.connection.mark_stream_done();

        Ok(data)
    }

    /// Data Type Fetch
    pub async fn data_type_fetch(
        &mut self,
        request: DataTypeFetchReq,
    ) -> Result<DataTypeFetchResp, RiakErr> {
        let bytes = request.serialize()?;

        let response = self
            .connection
            .exchange(codes::DtFetchReq, codes::DtFetchResp, &bytes)
            .await?;

        let data_type_fetch_resp = DtFetchResp::decode(response.as_slice())?;

        Ok(data_type_fetch_resp.into())
    }

    /// Secondary Index Request - Streaming
    pub async fn secondary_index_request_streaming(
        &mut self,
        mut req: IndexReq,
    ) -> Result<SecondaryIndexStream<'_>, RiakErr> {
        req.set_stream(true);
        SecondaryIndexStream::new(&mut self.connection, req).await
    }

    /// Secondary Index Request - Non-Streaming
    pub async fn secondary_index_request_non_streaming(
        &mut self,
        mut req: IndexReq,
    ) -> Result<IndexResp, RiakErr> {
        req.set_stream(false);

        let bytes = req.serialize()?;
        let response = self
            .connection
            .exchange(codes::RpbIndexReq, codes::RpbIndexResp, &bytes)
            .await?;

        let rpb_index_resp = RpbIndexResp::decode(response.as_slice())?;

        Ok(rpb_index_resp.into())
    }
}
