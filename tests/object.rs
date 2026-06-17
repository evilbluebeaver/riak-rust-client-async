#![allow(non_upper_case_globals)]
extern crate riak_async;

use riak_async::Client;
use riak_async::bucket::*;
use riak_async::object::*;

const bucket_type: &str = "tests/object.rs";
const bucket: &str = "test_bucket";
const key: &str = "test_key";

async fn connect() -> Client {
    Client::new("127.0.0.1:8087").await.unwrap()
}

#[tokio::test]
async fn test_store_object() {
    let mut riak = connect().await;
    setup_bucket_type(&mut riak, bucket_type).await;

    let mut content: ObjectContent = ObjectContent::new("Hello Riak!");
    content.set_content_type("text/plain");
    content.set_charset("UTF-8");

    let mut store_request = StoreObjectReq::new(bucket, content);
    store_request.set_bucket_type(bucket_type);
    store_request.set_key(key);
    store_request.set_dw(3);

    riak.store_object(store_request).await.unwrap();
}

#[tokio::test]
async fn test_fetch_object() {
    let mut riak = connect().await;
    setup_bucket_type(&mut riak, bucket_type).await;

    // store an object first
    let mut content = ObjectContent::new("Hello Riak!");
    content.set_content_type("text/plain");
    content.set_charset("UTF-8");

    let mut store_request = StoreObjectReq::new(bucket, content);
    store_request.set_bucket_type(bucket_type);
    store_request.set_key(key);
    store_request.set_dw(3);
    riak.store_object(store_request).await.unwrap();

    // fetch and verify
    let mut fetch_request = FetchObjectReq::new(bucket, key);
    fetch_request.set_bucket_type(bucket_type);

    let fetch_response = riak.fetch_object(fetch_request).await.unwrap();
    let content = fetch_response.content;
    println!("Number of siblings: {:?}", content.len());
}

async fn setup_bucket_type<T: Into<Vec<u8>>>(riak: &mut Client, bucket_type_name: T) {
    // convert the bucket_type_name to Vec<u8>
    let bucket_type_name = bucket_type_name.into();

    // build properties for the bucket type
    let mut props = BucketTypeProps::new(bucket_type_name);
    props.set_backend("leveldb");

    // set the properties for the bucket type
    riak.set_bucket_type_properties(props).await.unwrap();
}
