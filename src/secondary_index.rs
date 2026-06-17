use crate::errors::RiakErr;
use crate::proto::{RpbIndexReq, RpbIndexResp, rpb_index_req};
use prost::Message;

/// represents a type of 2i query
#[derive(Clone, Debug)]
pub enum IndexQueryType {
    EQ,
    RANGE,
}

impl From<IndexQueryType> for rpb_index_req::IndexQueryType {
    fn from(qtype: IndexQueryType) -> Self {
        match qtype {
            IndexQueryType::EQ => Self::Eq,
            IndexQueryType::RANGE => Self::Range,
        }
    }
}

/// represents a 2i request
#[derive(Clone, Debug)]
pub struct IndexReq(pub(crate) RpbIndexReq);

impl IndexReq {
    /// constructs a new `IndexReq`
    pub fn new<T: Into<Vec<u8>>>(
        bucket_type: T,
        bucket: T,
        index: T,
        qtype: IndexQueryType,
    ) -> IndexReq {
        let req = RpbIndexReq {
            r#type: Some(bucket_type.into()),
            bucket: bucket.into(),
            index: index.into(),
            qtype: rpb_index_req::IndexQueryType::from(qtype) as i32,
            ..Default::default()
        };
        IndexReq(req)
    }

    /// get the "pagination_sort" property
    pub fn get_pagination_sort(&self) -> Option<bool> {
        self.0.pagination_sort
    }

    /// set the "pagination_sort" property
    pub fn set_pagination_sort(&mut self, pagination_sort: bool) {
        self.0.pagination_sort = Some(pagination_sort);
    }

    /// get the "term_regex" property
    pub fn get_term_regex(&self) -> Option<&[u8]> {
        self.0.term_regex.as_deref()
    }

    /// set the "term_regex" property
    pub fn set_term_regex<T: Into<Vec<u8>>>(&mut self, term_regex: T) {
        self.0.term_regex = Some(term_regex.into());
    }

    /// get the "key" property
    pub fn get_key(&self) -> Option<&[u8]> {
        self.0.key.as_deref()
    }

    /// set the "key" property
    pub fn set_key<T: Into<Vec<u8>>>(&mut self, key: T) {
        self.0.key = Some(key.into());
    }

    /// get the "stream" property
    pub fn get_stream(&self) -> Option<bool> {
        self.0.stream
    }

    /// set the "stream" property
    pub fn set_stream(&mut self, stream: bool) {
        self.0.stream = Some(stream);
    }

    // TODO: implement the rest

    pub fn serialize(self) -> Result<Vec<u8>, RiakErr> {
        Ok(self.0.encode_to_vec())
    }
}

/// represents a 2i response
#[derive(Clone, Debug)]
pub struct IndexResp(#[allow(dead_code)] RpbIndexResp);

impl IndexResp {
    pub fn new(keys: Vec<Vec<u8>>) -> IndexResp {
        let resp = RpbIndexResp {
            keys,
            ..Default::default()
        };
        IndexResp(resp)
    }

    // TODO: implement the rest
}

impl From<RpbIndexResp> for IndexResp {
    fn from(resp: RpbIndexResp) -> Self {
        IndexResp(resp)
    }
}
