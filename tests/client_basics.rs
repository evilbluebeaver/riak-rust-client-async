extern crate riak_async;

use riak_async::Client;
use riak_async::bucket::{BucketProps, BucketTypeProps};
use riak_async::object::{DeleteObjectReq, FetchObjectReq, ObjectContent, StoreObjectReq};
use riak_async::yokozuna::{SearchQuery, YokozunaIndex};
use std::fs::File;
use std::io::Read;

const TEST_BUCKET: &str = "testbucket";

async fn connect() -> Client {
    let mut riak = Client::new("127.0.0.1:8087").await.unwrap();
    riak.ping().await.unwrap();
    riak
}

#[tokio::test]
async fn test_server_info() {
    let mut riak = connect().await;
    let (node, version) = riak.server_info().await.unwrap();
    println!(
        "connected to node {} running Riak version {}",
        node, version
    );
}

#[tokio::test]
async fn test_bucket_properties() {
    let mut riak = connect().await;

    let mut bucket_props = BucketProps::new(TEST_BUCKET);
    bucket_props.set_backend("leveldb");
    riak.set_bucket_properties(bucket_props).await.unwrap();

    let bucket_props = riak.get_bucket_properties(TEST_BUCKET).await.unwrap();
    let found_backend = bucket_props.get_backend().unwrap();
    assert_eq!(found_backend, "leveldb".as_bytes());
}

#[tokio::test]
async fn test_store_and_fetch_object() {
    let mut riak = connect().await;
    let key = "testkey_store_and_fetch";

    let contents = ObjectContent::new("this is a test".as_bytes());
    let mut req = StoreObjectReq::new(TEST_BUCKET, contents);
    req.set_key(key);
    riak.store_object(req).await.unwrap();

    let req = FetchObjectReq::new(TEST_BUCKET, key);
    let object = riak.fetch_object(req).await.unwrap();
    let contents = object.content;
    let content = contents.first().unwrap();
    assert_eq!(content.get_value(), "this is a test".as_bytes());
}

#[tokio::test]
async fn test_delete_object() {
    let mut riak = connect().await;
    let key = "testkey_delete";

    let contents = ObjectContent::new("this is a test".as_bytes());
    let mut req = StoreObjectReq::new(TEST_BUCKET, contents);
    req.set_key(key);
    riak.store_object(req).await.unwrap();

    let req = DeleteObjectReq::new(TEST_BUCKET, key);
    riak.delete_object(req).await.unwrap();

    let req = FetchObjectReq::new(TEST_BUCKET, key);
    let object = riak.fetch_object(req).await.unwrap();
    assert_eq!(object.content.len(), 0);
}

#[tokio::test]
async fn test_list_buckets() {
    let mut riak = connect().await;
    let key = "testkey_list_buckets";

    // ensure testbucket exists by storing an object
    let contents = ObjectContent::new("list_buckets_test".as_bytes());
    let mut req = StoreObjectReq::new(TEST_BUCKET, contents);
    req.set_key(key);
    riak.store_object(req).await.unwrap();

    let buckets = riak.list_buckets().await.unwrap();
    let mut bucket_exists = false;
    for bucket in buckets.iter() {
        if *bucket == TEST_BUCKET.as_bytes() {
            bucket_exists = true;
        }
    }
    assert!(bucket_exists);
}

#[tokio::test]
async fn test_list_keys() {
    let mut riak = connect().await;
    let key = "testkey_list_keys";

    // ensure testkey exists in testbucket
    let contents = ObjectContent::new("list_keys_test".as_bytes());
    let mut req = StoreObjectReq::new(TEST_BUCKET, contents);
    req.set_key(key);
    riak.store_object(req).await.unwrap();

    let keys = riak.list_keys(TEST_BUCKET).await.unwrap();
    let mut key_exists = false;
    for stored_key in keys.iter() {
        if *stored_key == key.as_bytes() {
            key_exists = true;
        }
    }
    assert!(key_exists);
}

#[tokio::test]
async fn test_stream_keys() {
    let mut riak = connect().await;
    let key = "testkey_stream_keys";

    // ensure testkey exists in testbucket
    let contents = ObjectContent::new("stream_keys_test".as_bytes());
    let mut req = StoreObjectReq::new(TEST_BUCKET, contents);
    req.set_key(key);
    riak.store_object(req).await.unwrap();

    let mut key_stream = riak.stream_keys(TEST_BUCKET).await.unwrap();
    let mut found_keys: Vec<Vec<u8>> = Vec::new();
    while let Some(batch) = key_stream.next().await {
        let batch = batch.unwrap();
        found_keys.extend(batch);
    }
    let key_exists = found_keys.iter().any(|k| k == key.as_bytes());
    assert!(key_exists);
}

#[tokio::test]
async fn test_stream_keys_interrupted_breaks_connection() {
    let mut riak = connect().await;

    // Store enough keys so the stream is likely to emit multiple chunks.
    for i in 0..200 {
        let key = format!("stream_interrupt_key_{i:04}");
        let value = format!("stream interrupt value {i}");
        let contents = ObjectContent::new(value.as_bytes());
        let mut req = StoreObjectReq::new(TEST_BUCKET, contents);
        req.set_key(key);
        riak.store_object(req).await.unwrap();
    }

    // Start streaming and consume just one batch, then drop the stream early.
    {
        let mut key_stream = riak.stream_keys(TEST_BUCKET).await.unwrap();
        let first_batch = key_stream
            .next()
            .await
            .expect("expected at least one stream batch")
            .unwrap();
        assert!(!first_batch.is_empty());
    }

    // The connection should still have unread stream data and be marked broken.
    assert!(riak.is_connection_broken());

    // Any follow-up request should fail until the caller reconnects.
    assert!(riak.ping().await.is_err());
}

#[tokio::test]
async fn test_fetch_preflist() {
    let mut riak = connect().await;
    let key = "testkey_fetch_preflist";

    // ensure testkey exists in testbucket
    let contents = ObjectContent::new("preflist_test".as_bytes());
    let mut req = StoreObjectReq::new(TEST_BUCKET, contents);
    req.set_key(key);
    riak.store_object(req).await.unwrap();

    let preflist = riak.fetch_preflist(TEST_BUCKET, key).await.unwrap();
    let mut lives_on_nodes: u8 = 0;
    let mut has_primary_node = false;
    for preflist_item in preflist.iter() {
        lives_on_nodes += 1;
        if preflist_item.is_primary {
            has_primary_node = true;
        }
    }
    assert_eq!(lives_on_nodes, 3);
    assert!(has_primary_node);
}

#[tokio::test]
async fn test_bucket_type_properties() {
    let mut riak = connect().await;

    let mut bucket_props = BucketTypeProps::new("testbuckettype");
    bucket_props.set_backend("leveldb");
    riak.set_bucket_type_properties(bucket_props).await.unwrap();

    let bucket_props = riak
        .get_bucket_type_properties("testbuckettype")
        .await
        .unwrap();
    assert_eq!(
        bucket_props.get_backend().expect("could not get backend"),
        "leveldb".as_bytes()
    );
}

#[tokio::test]
async fn test_yokozuna_schema() {
    let mut riak = connect().await;

    let mut xml: Vec<u8> = Vec::new();
    let mut file = File::open("tests/default-schema.xml").unwrap();
    let _ = file.read_to_end(&mut xml).unwrap();

    let schema_name = "schedule".to_string().into_bytes();
    riak.set_yokozuna_schema(schema_name.clone(), xml.clone())
        .await
        .unwrap();

    let schema = riak.get_yokozuna_schema(schema_name.clone()).await.unwrap();
    assert_eq!(schema, xml);
}

#[tokio::test]
async fn test_yokozuna_index() {
    let mut riak = connect().await;

    // ensure the schema exists first
    let mut xml: Vec<u8> = Vec::new();
    let mut file = File::open("tests/default-schema.xml").unwrap();
    let _ = file.read_to_end(&mut xml).unwrap();
    let schema_name = "schedule".to_string().into_bytes();
    riak.set_yokozuna_schema(schema_name.clone(), xml)
        .await
        .unwrap();

    let index_name = "myindex".to_string().into_bytes();
    let mut index = YokozunaIndex::new(index_name.clone());
    index.set_schema(schema_name);
    index.set_n_val(3);
    riak.set_yokozuna_index(index).await.unwrap();

    let index = riak.get_yokozuna_index(index_name.clone()).await.unwrap();
    assert_eq!(index[0].get_name(), index_name);
}

#[tokio::test]
async fn test_search() {
    let mut riak = connect().await;

    // ensure schema and index exist
    let mut xml: Vec<u8> = Vec::new();
    let mut file = File::open("tests/default-schema.xml").unwrap();
    let _ = file.read_to_end(&mut xml).unwrap();
    let schema_name = "schedule".to_string().into_bytes();
    riak.set_yokozuna_schema(schema_name.clone(), xml)
        .await
        .unwrap();

    let index_name = "myindex".to_string().into_bytes();
    let mut index = YokozunaIndex::new(index_name);
    index.set_schema(schema_name);
    index.set_n_val(3);
    riak.set_yokozuna_index(index).await.unwrap();

    let mut query = SearchQuery::new("test*", "myindex");
    query.set_df("_yz_id");
    riak.search(query).await.unwrap();
}

#[tokio::test]
async fn test_mapreduce() {
    let mut riak = connect().await;

    let key = "testkey_mapreduce";
    let contents = ObjectContent::new("mapreduce_test".as_bytes());
    let mut req = StoreObjectReq::new(TEST_BUCKET, contents);
    req.set_key(key);
    riak.store_object(req).await.unwrap();

    let job = [
        r#"
    {"inputs": [[""#,
        TEST_BUCKET,
        r#"", ""#,
        key,
        r#""]], "query": [
            {"map": {
                "arg": null,
            "name": "Riak.mapValues",
                "language": "javascript",
                "keep": true
        }}
    ]}
    "#,
    ]
    .concat();

    riak.mapreduce(job.as_str(), "application/json")
        .await
        .unwrap();
}
