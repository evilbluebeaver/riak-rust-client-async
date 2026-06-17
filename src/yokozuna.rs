/// Riak Search integrates Apache Solr for indexing and querying Riak KV
///
/// For more information: https://docs.basho.com/riak/kv/latest/developing/usage/search/
use crate::errors::RiakErr;
use crate::proto::{
    RpbPair, RpbSearchQueryReq, RpbSearchQueryResp, RpbYokozunaIndex, RpbYokozunaIndexPutReq,
};
use prost::Message;

/// `YokozunaIndex` represents an index for Riak search
#[derive(Clone, Debug)]
pub struct YokozunaIndex(RpbYokozunaIndex);

impl YokozunaIndex {
    /// constructs a new `YokozunaIndex`
    pub fn new<T: Into<Vec<u8>>>(name: T) -> YokozunaIndex {
        let rpb_yokozuna_index = RpbYokozunaIndex {
            name: name.into(),
            ..Default::default()
        };
        YokozunaIndex(rpb_yokozuna_index)
    }

    /// get the "name" property
    pub fn get_name(&self) -> &[u8] {
        &self.0.name
    }

    /// set the "name" property
    pub fn set_name<T: Into<Vec<u8>>>(&mut self, name: T) {
        self.0.name = name.into();
    }

    /// get the "schema" property
    pub fn get_schema(&self) -> Option<&[u8]> {
        self.0.schema.as_deref()
    }

    /// set the "schema" property
    pub fn set_schema<T: Into<Vec<u8>>>(&mut self, schema: T) {
        self.0.schema = Some(schema.into());
    }

    /// get the "n_val" property
    pub fn get_n_val(&self) -> Option<u32> {
        self.0.n_val
    }

    /// set the "n_val" property
    pub fn set_n_val(&mut self, n_val: u32) {
        self.0.n_val = Some(n_val);
    }

    pub fn serialize(self) -> Result<Vec<u8>, RiakErr> {
        let req = RpbYokozunaIndexPutReq {
            index: self.0,
            timeout: None,
        };
        Ok(req.encode_to_vec())
    }
}

impl From<RpbYokozunaIndex> for YokozunaIndex {
    fn from(rpb_yokozuna_index: RpbYokozunaIndex) -> Self {
        YokozunaIndex(rpb_yokozuna_index)
    }
}

/// `SearchQuery` represents a query that can be performed on Riak
#[derive(Clone, Debug)]
pub struct SearchQuery(RpbSearchQueryReq);

impl SearchQuery {
    /// constructs a new `SearchQuery`
    pub fn new<T: Into<Vec<u8>>>(q: T, index: T) -> SearchQuery {
        let req = RpbSearchQueryReq {
            q: q.into(),
            index: index.into(),
            ..Default::default()
        };
        SearchQuery(req)
    }

    /// get the "q" property
    pub fn get_q(&self) -> &[u8] {
        &self.0.q
    }

    /// set the "q" property
    pub fn set_q<T: Into<Vec<u8>>>(&mut self, q: T) {
        self.0.q = q.into();
    }

    /// get the "index" property
    pub fn get_index(&self) -> &[u8] {
        &self.0.index
    }

    /// set the "index" property
    pub fn set_index<T: Into<Vec<u8>>>(&mut self, index: T) {
        self.0.index = index.into();
    }

    /// get the "rows" property
    pub fn get_rows(&self) -> Option<u32> {
        self.0.rows
    }

    /// set the "rows" property
    pub fn set_rows(&mut self, rows: u32) {
        self.0.rows = Some(rows);
    }

    /// get the "start" property
    pub fn get_start(&self) -> Option<u32> {
        self.0.start
    }

    /// set the "start" property
    pub fn set_start(&mut self, start: u32) {
        self.0.start = Some(start);
    }

    /// get the "sort" property
    pub fn get_sort(&self) -> Option<&[u8]> {
        self.0.sort.as_deref()
    }

    /// set the "sort" property
    pub fn set_sort<T: Into<Vec<u8>>>(&mut self, sort: T) {
        self.0.sort = Some(sort.into());
    }

    /// get the "filter" property
    pub fn get_filter(&self) -> Option<&[u8]> {
        self.0.filter.as_deref()
    }

    /// set the "filter" property
    pub fn set_filter<T: Into<Vec<u8>>>(&mut self, filter: T) {
        self.0.filter = Some(filter.into());
    }

    /// get the "df" property
    pub fn get_df(&self) -> Option<&[u8]> {
        self.0.df.as_deref()
    }

    /// set the "df" property
    pub fn set_df<T: Into<Vec<u8>>>(&mut self, df: T) {
        self.0.df = Some(df.into());
    }

    /// get the "op" property
    pub fn get_op(&self) -> Option<&[u8]> {
        self.0.op.as_deref()
    }

    /// set the "op" property
    pub fn set_op<T: Into<Vec<u8>>>(&mut self, op: T) {
        self.0.op = Some(op.into());
    }

    /// get the "fl" property
    pub fn get_fl(&self) -> Option<&[Vec<u8>]> {
        if self.0.filter.is_some() {
            return None;
        }
        Some(&self.0.fl)
    }

    /// set the "fl" property
    pub fn set_fl<T: Into<Vec<Vec<u8>>>>(&mut self, fl: T) {
        self.0.fl = fl.into();
    }

    /// get the "presort" property
    pub fn get_presort(&self) -> Option<&[u8]> {
        self.0.presort.as_deref()
    }

    /// set the "presort" property
    pub fn set_presort<T: Into<Vec<u8>>>(&mut self, presort: T) {
        self.0.presort = Some(presort.into());
    }

    pub fn serialize(self) -> Result<Vec<u8>, RiakErr> {
        Ok(self.0.encode_to_vec())
    }
}

/// `SearchQueryResp` represents the response for a successful query
#[derive(Clone, Debug)]
pub struct SearchQueryResp {
    pub pairs: Vec<(Vec<u8>, Option<Vec<u8>>)>,
    pub max_score: Option<f32>,
    pub num_found: Option<u32>,
}

impl From<RpbSearchQueryResp> for SearchQueryResp {
    fn from(
        RpbSearchQueryResp {
            docs,
            max_score,
            num_found,
        }: RpbSearchQueryResp,
    ) -> Self {
        let pairs = docs
            .into_iter()
            .flat_map(|doc| doc.fields)
            .map(Into::into)
            .collect();

        SearchQueryResp {
            pairs,
            max_score,
            num_found,
        }
    }
}

impl From<RpbPair> for (Vec<u8>, Option<Vec<u8>>) {
    fn from(RpbPair { key, value }: RpbPair) -> Self {
        (key, value)
    }
}
