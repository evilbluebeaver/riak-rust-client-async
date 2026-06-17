use std::time::Duration;

use crate::errors::RiakErr;
use crate::proto::{
    DtFetchReq, DtFetchResp, DtUpdateReq, DtValue, MapEntry, MapField, dt_fetch_resp, map_field,
};
use prost::Message;

/// represents a request to store a Data Type object
#[derive(Clone, Debug)]
pub struct DataTypeUpdateReq(#[allow(dead_code)] DtUpdateReq);

impl DataTypeUpdateReq {
    /// constructs a new `DataTypeUpdateReq`
    pub fn new<T: Into<Vec<u8>>>(bucket: T, bucket_type: T) -> DataTypeUpdateReq {
        let req = DtUpdateReq {
            bucket: bucket.into(),
            r#type: bucket_type.into(),
            ..Default::default()
        };
        DataTypeUpdateReq(req)
    }
}

/// represents a request to fetch an object via Riak Data Types
#[derive(Clone, Debug)]
pub struct DataTypeFetchReq(DtFetchReq);

impl DataTypeFetchReq {
    /// constructs a new `DataTypeFetchReq`
    pub fn new<T: Into<Vec<u8>>>(bucket_type: T, bucket: T, key: T) -> DataTypeFetchReq {
        let req = DtFetchReq {
            bucket: bucket.into(),
            r#type: bucket_type.into(),
            key: key.into(),
            ..Default::default()
        };
        DataTypeFetchReq(req)
    }

    /// get the "bucket_type" property
    pub fn get_bucket_type(&self) -> &[u8] {
        &self.0.r#type
    }

    /// set the "bucket_type" property
    pub fn set_bucket_type<T: Into<Vec<u8>>>(&mut self, bucket_type: T) {
        self.0.r#type = bucket_type.into();
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

    /// get the "timeout" property
    pub fn get_timeout(&self) -> Option<Duration> {
        self.0.timeout.map(Into::into).map(Duration::from_millis)
    }

    /// set the "timeout" property
    pub fn set_timeout(&mut self, timeout: Duration) {
        self.0.timeout = Some(timeout.as_millis() as u32);
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

    /// get the "include_context" property
    pub fn get_include_context(&self) -> Option<bool> {
        self.0.include_context
    }

    /// set the "include_context" property
    pub fn set_include_context(&mut self, include_context: bool) {
        self.0.include_context = Some(include_context);
    }

    pub fn serialize(self) -> Result<Vec<u8>, RiakErr> {
        Ok(self.0.encode_to_vec())
    }
}

/// represents the valid data types
#[derive(Clone, Copy, Debug, Default)]
pub enum DataType {
    #[default]
    COUNTER,
    SET,
    MAP,
    HLL,
}

impl From<dt_fetch_resp::DataType> for DataType {
    fn from(dt_type: dt_fetch_resp::DataType) -> Self {
        match dt_type {
            dt_fetch_resp::DataType::Counter => Self::COUNTER,
            dt_fetch_resp::DataType::Set => Self::SET,
            dt_fetch_resp::DataType::Map => Self::MAP,
            dt_fetch_resp::DataType::Hll => Self::HLL,
            //TODO: handle GSET
            _ => Self::COUNTER, // fallback
        }
    }
}

/// represents the response from a `DTFetchReq`
#[derive(Clone, Debug)]
pub struct DataTypeFetchResp {
    pub context: Option<Vec<u8>>,
    pub data_type: DataType,
    pub value: Option<DataTypeValue>,
}

impl From<DtFetchResp> for DataTypeFetchResp {
    fn from(
        DtFetchResp {
            mut context,
            r#type,
            mut value,
        }: DtFetchResp,
    ) -> Self {
        let context = context.take();
        let data_type = dt_fetch_resp::DataType::try_from(r#type)
            .map(Into::into)
            .unwrap_or_default();
        let value = value.take().map(DataTypeValue::from);

        DataTypeFetchResp {
            context,
            data_type,
            value,
        }
    }
}

/// represents a map entry
#[derive(Clone, Debug)]
pub struct DataTypeMapEntry {
    pub field: DataTypeMapField,
    pub counter_value: Option<i64>,
    pub set_value: Vec<Vec<u8>>,
    pub register_value: Option<Vec<u8>>,
    pub flag_value: Option<bool>,
    pub map_value: Vec<DataTypeMapEntry>,
}

impl From<MapEntry> for DataTypeMapEntry {
    fn from(
        MapEntry {
            field,
            counter_value,
            set_value,
            register_value,
            flag_value,
            map_value,
        }: MapEntry,
    ) -> Self {
        let field = DataTypeMapField::from(field);
        let map_value = map_value.into_iter().map(Into::into).collect();

        DataTypeMapEntry {
            field,
            counter_value,
            set_value,
            register_value,
            flag_value,
            map_value,
        }
    }
}

/// represents the map field
#[derive(Clone, Debug)]
pub struct DataTypeMapField {
    pub name: Vec<u8>,
    pub field_type: DataTypeMapFieldType,
}

impl From<MapField> for DataTypeMapField {
    fn from(MapField { name, r#type }: MapField) -> Self {
        let field_type = map_field::MapFieldType::try_from(r#type)
            .map(Into::into)
            .unwrap_or_default();

        DataTypeMapField { name, field_type }
    }
}

/// represents the types valid for a map field
#[derive(Clone, Copy, Debug, Default)]
pub enum DataTypeMapFieldType {
    #[default]
    COUNTER,
    SET,
    REGISTER,
    FLAG,
    MAP,
}

impl From<map_field::MapFieldType> for DataTypeMapFieldType {
    fn from(field_type: map_field::MapFieldType) -> Self {
        match field_type {
            map_field::MapFieldType::Counter => Self::COUNTER,
            map_field::MapFieldType::Set => Self::SET,
            map_field::MapFieldType::Register => Self::REGISTER,
            map_field::MapFieldType::Flag => Self::FLAG,
            map_field::MapFieldType::Map => Self::MAP,
        }
    }
}

/// represents the value of a data-type
#[derive(Clone, Debug)]
pub struct DataTypeValue {
    pub counter_value: Option<i64>,
    pub set_value: Vec<Vec<u8>>,
    pub map_value: Vec<DataTypeMapEntry>,
}

impl From<DtValue> for DataTypeValue {
    fn from(
        DtValue {
            counter_value,
            set_value,
            map_value,
            // TODO - determine what to do with hll_value and gset_value
            ..
        }: DtValue,
    ) -> Self {
        let map_value = map_value.into_iter().map(DataTypeMapEntry::from).collect();

        DataTypeValue {
            counter_value,
            set_value,
            map_value,
        }
    }
}
