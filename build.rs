fn main() {
    prost_build::Config::new()
        .compile_protos(
            &[
                "proto/riak.proto",
                "proto/riak_kv.proto",
                "proto/riak_dt.proto",
                "proto/riak_search.proto",
                "proto/riak_yokozuna.proto",
            ],
            &["proto/"],
        )
        .unwrap();
}
