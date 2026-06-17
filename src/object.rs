/// Object related structs and functions for Riak objects.
///
/// For more information: https://docs.basho.com/riak/kv/latest/developing/usage/creating-objects/
use crate::errors::RiakErr;
use crate::proto::{RpbContent, RpbDelReq, RpbGetReq, RpbGetResp, RpbPair, RpbPutReq};
use prost::Message;

/// A list of key-value index pairs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Indexes(pub Vec<(Vec<u8>, Vec<u8>)>);

impl Indexes {
    pub fn from_proto(pairs: &[RpbPair]) -> Option<Self> {
        if pairs.is_empty() {
            return None;
        }
        Some(Self::from(pairs.to_vec()))
    }
}

impl From<Vec<RpbPair>> for Indexes {
    fn from(pairs: Vec<RpbPair>) -> Self {
        Self(
            pairs
                .into_iter()
                .map(|p| (p.key, p.value.unwrap_or_default()))
                .collect(),
        )
    }
}

impl From<Indexes> for Vec<RpbPair> {
    fn from(indexes: Indexes) -> Self {
        indexes
            .0
            .into_iter()
            .map(|(key, value)| RpbPair {
                key,
                value: Some(value),
            })
            .collect()
    }
}

/// `DeleteObjectReq` represents a request to delete an object from Riak
#[derive(Clone, Debug)]
pub struct DeleteObjectReq(RpbDelReq);

impl DeleteObjectReq {
    /// constructs a new `DeleteObjectReq`
    pub fn new<T: Into<Vec<u8>>>(bucket: T, key: T) -> DeleteObjectReq {
        let req = RpbDelReq {
            bucket: bucket.into(),
            key: key.into(),
            ..Default::default()
        };
        DeleteObjectReq(req)
    }

    /// get the the "bucket_type" property
    pub fn get_bucket_type(&self) -> Option<&[u8]> {
        self.0.r#type.as_deref()
    }

    /// set the the "bucket_type" property
    pub fn set_bucket_type<T: Into<Vec<u8>>>(&mut self, bucket_type: T) {
        self.0.r#type = Some(bucket_type.into());
    }

    /// get the the "bucket" property
    pub fn get_bucket(&self) -> &[u8] {
        &self.0.bucket
    }

    /// set the the "bucket" property
    pub fn set_bucket<T: Into<Vec<u8>>>(&mut self, bucket: T) {
        self.0.bucket = bucket.into();
    }

    /// get the the "key" property
    pub fn get_key(&self) -> &[u8] {
        &self.0.key
    }

    /// set the the "key" property
    pub fn set_key<T: Into<Vec<u8>>>(&mut self, key: T) {
        self.0.key = key.into();
    }

    /// get the the "dw" property
    pub fn get_dw(&self) -> Option<u32> {
        self.0.dw
    }

    /// set the the "dw" property
    pub fn set_dw(&mut self, dw: u32) {
        self.0.dw = Some(dw);
    }

    /// get the the "pr" property
    pub fn get_pr(&self) -> Option<u32> {
        self.0.pr
    }

    /// set the the "pr" property
    pub fn set_pr(&mut self, pr: u32) {
        self.0.pr = Some(pr);
    }

    /// get the the "pw" property
    pub fn get_pw(&self) -> Option<u32> {
        self.0.pw
    }

    /// set the the "pw" property
    pub fn set_pw(&mut self, pw: u32) {
        self.0.pw = Some(pw);
    }

    /// get the the "rw" property
    pub fn get_rw(&self) -> Option<u32> {
        self.0.rw
    }

    /// set the the "rw" property
    pub fn set_rw(&mut self, rw: u32) {
        self.0.rw = Some(rw);
    }

    /// get the the "r" property
    pub fn get_r(&self) -> Option<u32> {
        self.0.r
    }

    /// set the the "r" property
    pub fn set_r(&mut self, r: u32) {
        self.0.r = Some(r);
    }

    /// get the the "w" property
    pub fn get_w(&self) -> Option<u32> {
        self.0.w
    }

    /// set the the "w" property
    pub fn set_w(&mut self, w: u32) {
        self.0.w = Some(w);
    }

    /// get the the "n_val" property
    pub fn get_n_val(&self) -> Option<u32> {
        self.0.n_val
    }

    /// set the the "n_val" property
    pub fn set_n_val(&mut self, n_val: u32) {
        self.0.n_val = Some(n_val);
    }

    /// get the the "sloppy_quorum" property
    pub fn get_sloppy_quorum(&self) -> Option<bool> {
        self.0.sloppy_quorum
    }

    /// set the the "sloppy_quorum" property
    pub fn set_sloppy_quorum(&mut self, sloppy_quorum: bool) {
        self.0.sloppy_quorum = Some(sloppy_quorum);
    }

    /// get the the "timeout" property
    pub fn get_timeout(&self) -> Option<u32> {
        self.0.timeout
    }

    /// set the the "timeout" property
    pub fn set_timeout(&mut self, timeout: u32) {
        self.0.timeout = Some(timeout);
    }

    /// get the the "vclock" property
    pub fn get_vclock(&self) -> Option<&[u8]> {
        self.0.vclock.as_deref()
    }

    /// set the the "vclock" property
    pub fn set_vclock<T: Into<Vec<u8>>>(&mut self, vclock: T) {
        self.0.vclock = Some(vclock.into());
    }

    pub fn serialize(self) -> Result<Vec<u8>, RiakErr> {
        Ok(self.0.encode_to_vec())
    }
}

/// `StoreObjectReq` represents the data used to store an object in Riak
#[derive(Clone, Debug)]
pub struct StoreObjectReq(RpbPutReq, ObjectContent);

impl StoreObjectReq {
    /// constructs a new `StoreObjectReq`
    pub fn new<T: Into<Vec<u8>>>(bucket: T, content: ObjectContent) -> StoreObjectReq {
        let rpb_put_req = RpbPutReq {
            bucket: bucket.into(),
            ..Default::default()
        };
        StoreObjectReq(rpb_put_req, content)
    }

    /// get the "bucket" property
    pub fn get_bucket(&self) -> &[u8] {
        &self.0.bucket
    }

    /// set the "bucket" property
    pub fn set_bucket<T: Into<Vec<u8>>>(&mut self, bucket: T) {
        self.0.bucket = bucket.into();
    }

    /// get the "content" property
    pub fn get_content(&self) -> &ObjectContent {
        &self.1
    }

    /// set the "content" property
    pub fn set_content(&mut self, content: ObjectContent) {
        self.1 = content;
    }

    /// get the "asis" property
    pub fn get_asis(&self) -> Option<bool> {
        self.0.asis
    }

    /// set the "asis" property
    pub fn set_asis(&mut self, asis: bool) {
        self.0.asis = Some(asis);
    }

    /// get the "bucket_type" property
    pub fn get_bucket_type(&self) -> Option<&[u8]> {
        self.0.r#type.as_deref()
    }

    /// set the "bucket_type" property
    pub fn set_bucket_type<T: Into<Vec<u8>>>(&mut self, bucket_type: T) {
        self.0.r#type = Some(bucket_type.into());
    }

    /// get the "if_none_match" property
    pub fn get_if_none_match(&self) -> Option<bool> {
        self.0.if_none_match
    }

    /// set the "if_none_match" property
    pub fn set_if_none_match(&mut self, if_none_match: bool) {
        self.0.if_none_match = Some(if_none_match);
    }

    /// get the "if_not_modified" property
    pub fn get_if_not_modified(&self) -> Option<bool> {
        self.0.if_not_modified
    }

    /// set the "if_not_modified" property
    pub fn set_if_not_modified(&mut self, if_not_modified: bool) {
        self.0.if_not_modified = Some(if_not_modified);
    }

    /// get the "key" property
    pub fn get_key(&self) -> Option<&[u8]> {
        self.0.key.as_deref()
    }

    /// set the "key" property
    pub fn set_key<T: Into<Vec<u8>>>(&mut self, key: T) {
        self.0.key = Some(key.into());
    }

    /// get the "n_val" property
    pub fn get_n_val(&self) -> Option<u32> {
        self.0.n_val
    }

    /// set the "n_val" property
    pub fn set_n_val(&mut self, n_val: u32) {
        self.0.n_val = Some(n_val);
    }

    /// get the "return_body" property
    pub fn get_return_body(&self) -> Option<bool> {
        self.0.return_body
    }

    /// set the "return_body" property
    pub fn set_return_body(&mut self, return_body: bool) {
        self.0.return_body = Some(return_body);
    }

    /// get the "return_head" property
    pub fn get_return_head(&self) -> Option<bool> {
        self.0.return_head
    }

    /// set the "return_head" property
    pub fn set_return_head(&mut self, return_head: bool) {
        self.0.return_head = Some(return_head);
    }

    /// get the "sloppy_quorum" property
    pub fn get_sloppy_quorum(&self) -> Option<bool> {
        self.0.sloppy_quorum
    }

    /// set the "sloppy_quorum" property
    pub fn set_sloppy_quorum(&mut self, sloppy_quorum: bool) {
        self.0.sloppy_quorum = Some(sloppy_quorum);
    }

    /// get the "timeout" property
    pub fn get_timeout(&self) -> Option<u32> {
        self.0.timeout
    }

    /// set the "timeout" property
    pub fn set_timeout(&mut self, timeout: u32) {
        self.0.timeout = Some(timeout);
    }

    /// get the "vclock" property
    pub fn get_vclock(&self) -> Option<&[u8]> {
        self.0.vclock.as_deref()
    }

    /// set the "vclock" property
    pub fn set_vclock<T: Into<Vec<u8>>>(&mut self, vclock: T) {
        self.0.vclock = Some(vclock.into());
    }

    /// get the "dw" property
    pub fn get_dw(&self) -> Option<u32> {
        self.0.dw
    }

    /// set the "dw" property
    pub fn set_dw(&mut self, dw: u32) {
        self.0.dw = Some(dw);
    }

    /// get the "pw" property
    pub fn get_pw(&self) -> Option<u32> {
        self.0.pw
    }

    /// set the "pw" property
    pub fn set_pw(&mut self, pw: u32) {
        self.0.pw = Some(pw);
    }

    /// get the "w" property
    pub fn get_w(&self) -> Option<u32> {
        self.0.w
    }

    /// set the "w" property
    pub fn set_w(&mut self, w: u32) {
        self.0.w = Some(w);
    }

    pub fn serialize(mut self) -> Result<Vec<u8>, RiakErr> {
        self.0.content = self.1.into();
        Ok(self.0.encode_to_vec())
    }
}

/// `FetchObjectReq` represents the data used to perform a fetch object request
#[derive(Clone, Debug)]
pub struct FetchObjectReq(RpbGetReq);

impl FetchObjectReq {
    /// constructs a new `FetchObjectReq`
    pub fn new<T: Into<Vec<u8>>>(bucket: T, key: T) -> FetchObjectReq {
        let req = RpbGetReq {
            bucket: bucket.into(),
            key: key.into(),
            ..Default::default()
        };
        FetchObjectReq(req)
    }

    /// get the "bucket" property
    pub fn get_bucket(&self) -> &[u8] {
        &self.0.bucket
    }

    /// set the "bucket" property
    pub fn set_bucket<T: Into<Vec<u8>>>(&mut self, bucket: T) {
        self.0.bucket = bucket.into();
    }

    /// get the "key" property
    pub fn get_key(&self) -> &[u8] {
        &self.0.key
    }

    /// set the "key" property
    pub fn set_key<T: Into<Vec<u8>>>(&mut self, key: T) {
        self.0.key = key.into();
    }

    /// get the "r" property
    pub fn get_r(&self) -> Option<u32> {
        self.0.r
    }

    /// set the "r" property
    pub fn set_r(&mut self, r: u32) {
        self.0.r = Some(r);
    }

    /// get the "pr" property
    pub fn get_pr(&self) -> Option<u32> {
        self.0.pr
    }

    /// set the "pr" property
    pub fn set_pr(&mut self, pr: u32) {
        self.0.pr = Some(pr);
    }

    /// get the "basic_quorum" property
    pub fn get_basic_quorum(&self) -> Option<bool> {
        self.0.basic_quorum
    }

    /// set the "basic_quorum" property
    pub fn set_basic_quorum(&mut self, basic_quorum: bool) {
        self.0.basic_quorum = Some(basic_quorum);
    }

    /// get the "notfound_ok" property
    pub fn get_notfound_ok(&self) -> Option<bool> {
        self.0.notfound_ok
    }

    /// set the "notfound_ok" property
    pub fn set_notfound_ok(&mut self, notfound_ok: bool) {
        self.0.notfound_ok = Some(notfound_ok);
    }

    /// get the "if_modified" property
    pub fn get_if_modified(&self) -> Option<&[u8]> {
        self.0.if_modified.as_deref()
    }

    /// set the "if_modified" property
    pub fn set_if_modified<T: Into<Vec<u8>>>(&mut self, if_modified: T) {
        self.0.if_modified = Some(if_modified.into());
    }

    /// get the "head" property
    pub fn get_head(&self) -> Option<bool> {
        self.0.head
    }

    /// set the "head" property
    pub fn set_head(&mut self, head: bool) {
        self.0.head = Some(head);
    }

    /// get the "deleted_vclock" property
    pub fn get_deleted_vclock(&self) -> Option<bool> {
        self.0.deletedvclock
    }

    /// set the "deleted_vclock" property
    pub fn set_deleted_vclock(&mut self, deleted_vclock: bool) {
        self.0.deletedvclock = Some(deleted_vclock);
    }

    /// get the "timeout" property
    pub fn get_timeout(&self) -> Option<u32> {
        self.0.timeout
    }

    /// set the "timeout" property
    pub fn set_timeout(&mut self, timeout: u32) {
        self.0.timeout = Some(timeout);
    }

    /// get the "sloppy_quorum" property
    pub fn get_sloppy_quorum(&self) -> Option<bool> {
        self.0.sloppy_quorum
    }

    /// set the "sloppy_quorum" property
    pub fn set_sloppy_quorum(&mut self, sloppy_quorum: bool) {
        self.0.sloppy_quorum = Some(sloppy_quorum);
    }

    /// get the "n_val" property
    pub fn get_n_val(&self) -> Option<u32> {
        self.0.n_val
    }

    /// set the "n_val" property
    pub fn set_n_val(&mut self, n_val: u32) {
        self.0.n_val = Some(n_val);
    }

    /// get the "bucket_type" property
    pub fn get_bucket_type(&self) -> Option<&[u8]> {
        self.0.r#type.as_deref()
    }

    /// set the "bucket_type" property
    pub fn set_bucket_type<T: Into<Vec<u8>>>(&mut self, bucket_type: T) {
        self.0.r#type = Some(bucket_type.into());
    }

    pub fn serialize(self) -> Result<Vec<u8>, RiakErr> {
        Ok(self.0.encode_to_vec())
    }
}

/// `FetchObjectResp` represents the response received from Riak when storing an object
#[derive(Clone, Debug)]
pub struct FetchObjectResp {
    pub content: Vec<ObjectContent>,
    pub vclock: Vec<u8>,
    pub unchanged: Option<bool>,
}

impl From<RpbGetResp> for FetchObjectResp {
    fn from(
        RpbGetResp {
            content,
            vclock,
            unchanged,
        }: RpbGetResp,
    ) -> Self {
        let content = content.into_iter().map(ObjectContent::from).collect();

        FetchObjectResp {
            content,
            vclock: vclock.unwrap_or_default(),
            unchanged,
        }
    }
}

/// `ObjectContent` represents the contents of a Riak object
#[derive(Clone, Debug)]
pub struct ObjectContent(RpbContent);

impl ObjectContent {
    pub fn new<T: Into<Vec<u8>>>(value: T) -> ObjectContent {
        let rpb_content = RpbContent {
            value: value.into(),
            ..Default::default()
        };
        ObjectContent(rpb_content)
    }

    /// get the "value" property
    pub fn get_value(&self) -> &[u8] {
        &self.0.value
    }

    /// set the "value" property
    pub fn set_value<T: Into<Vec<u8>>>(&mut self, value: T) {
        self.0.value = value.into();
    }

    /// get the "content_type" property
    pub fn get_content_type(&self) -> Option<&[u8]> {
        self.0.content_type.as_deref()
    }

    /// set the "content_type" property
    pub fn set_content_type<T: Into<Vec<u8>>>(&mut self, content_type: T) {
        self.0.content_type = Some(content_type.into());
    }

    /// get the "charset" property
    pub fn get_charset(&self) -> Option<&[u8]> {
        self.0.charset.as_deref()
    }

    /// set the "charset" property
    pub fn set_charset<T: Into<Vec<u8>>>(&mut self, charset: T) {
        self.0.charset = Some(charset.into());
    }

    /// get the "content_encoding" property
    pub fn get_content_encoding(&self) -> Option<&[u8]> {
        self.0.content_encoding.as_deref()
    }

    /// set the "content_encoding" property
    pub fn set_content_encoding<T: Into<Vec<u8>>>(&mut self, content_encoding: T) {
        self.0.content_encoding = Some(content_encoding.into());
    }

    /// get the "vtag" property
    pub fn get_vtag(&self) -> Option<&[u8]> {
        self.0.vtag.as_deref()
    }

    /// set the "vtag" property
    pub fn set_vtag<T: Into<Vec<u8>>>(&mut self, vtag: T) {
        self.0.vtag = Some(vtag.into());
    }

    /// get the "last_mod" property
    pub fn get_last_mod(&self) -> Option<u32> {
        self.0.last_mod
    }

    /// set the "last_mod" property
    pub fn set_last_mod(&mut self, last_mod: u32) {
        self.0.last_mod = Some(last_mod);
    }

    /// get the "last_mod_usecs" property
    pub fn get_last_mod_usecs(&self) -> Option<u32> {
        self.0.last_mod_usecs
    }

    /// set the "last_mod_usecs" property
    pub fn set_last_mod_usecs(&mut self, last_mod_usecs: u32) {
        self.0.last_mod_usecs = Some(last_mod_usecs);
    }

    /// get the "deleted" property
    pub fn get_deleted(&self) -> Option<bool> {
        self.0.deleted
    }

    /// set the "deleted" property
    pub fn set_deleted(&mut self, deleted: bool) {
        self.0.deleted = Some(deleted);
    }

    /// get the "indexes" property
    pub fn get_indexes(&self) -> Option<Indexes> {
        Indexes::from_proto(&self.0.indexes)
    }

    /// set the "indexes" property
    pub fn set_indexes(&mut self, indexes: Indexes) {
        self.0.indexes = indexes.into();
    }
}

impl From<ObjectContent> for RpbContent {
    fn from(content: ObjectContent) -> Self {
        content.0
    }
}

impl From<RpbContent> for ObjectContent {
    fn from(rpb_content: RpbContent) -> Self {
        ObjectContent(rpb_content)
    }
}
