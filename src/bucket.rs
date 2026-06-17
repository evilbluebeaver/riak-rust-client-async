/// Bucket related structs for dealing with Riak buckets.
///
/// For more information: https://docs.basho.com/riak/kv/latest/learn/concepts/buckets/
///
use crate::errors::RiakErr;
use crate::proto::{
    RpbBucketProps, RpbCommitHook, RpbModFun, RpbSetBucketReq, RpbSetBucketTypeReq,
};
use prost::Message;
use std::collections::HashMap;

/// A map of commit hook names to their (module, function) pairs.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommitHooks(pub HashMap<Vec<u8>, (Vec<u8>, Vec<u8>)>);

impl CommitHooks {
    pub fn from_proto(hooks: &[RpbCommitHook]) -> Option<Self> {
        if hooks.is_empty() {
            return None;
        }
        Some(hooks.into())
    }
}

impl From<&[RpbCommitHook]> for CommitHooks {
    fn from(hooks: &[RpbCommitHook]) -> Self {
        let mut map = HashMap::with_capacity(hooks.len());
        for RpbCommitHook { modfun, name } in hooks {
            let modfun = modfun.clone().unwrap_or_default();
            let name = name.clone().unwrap_or_default();
            map.insert(name, (modfun.module, modfun.function));
        }
        Self(map)
    }
}

impl From<CommitHooks> for Vec<RpbCommitHook> {
    fn from(hooks: CommitHooks) -> Self {
        hooks.0.into_iter().map(Into::into).collect()
    }
}

impl From<(Vec<u8>, (Vec<u8>, Vec<u8>))> for RpbCommitHook {
    fn from((name, (module, function)): (Vec<u8>, (Vec<u8>, Vec<u8>))) -> Self {
        RpbCommitHook {
            name: Some(name),
            modfun: Some(RpbModFun { module, function }),
        }
    }
}

impl<T: Into<Vec<u8>>> From<(T, T)> for RpbModFun {
    fn from((module, function): (T, T)) -> Self {
        RpbModFun {
            module: module.into(),
            function: function.into(),
        }
    }
}

impl<'a> From<&'a RpbModFun> for (&'a [u8], &'a [u8]) {
    fn from(modfun: &'a RpbModFun) -> Self {
        (modfun.module.as_slice(), modfun.function.as_slice())
    }
}

/// `BucketProps` represents the properties that can bet set on a bucket
#[derive(Clone, Debug)]
pub struct BucketProps(RpbBucketProps, Vec<u8>);

impl BucketProps {
    /// constructs a new `BucketProps`
    pub fn new<T: Into<Vec<u8>>>(bucket_name: T) -> BucketProps {
        BucketProps(RpbBucketProps::default(), bucket_name.into())
    }
    pub fn new_with_props<T: Into<Vec<u8>>>(bucket_name: T, props: RpbBucketProps) -> BucketProps {
        BucketProps(props, bucket_name.into())
    }

    /// get the "bucket" property
    pub fn get_bucket(&self) -> &[u8] {
        self.1.as_slice()
    }

    /// set the "bucket" property
    pub fn set_bucket<T: Into<Vec<u8>>>(&mut self, bucket: T) {
        self.1 = bucket.into();
    }

    /// get the "allow_mult" property
    pub fn get_allow_mult(&self) -> Option<bool> {
        self.0.allow_mult
    }

    /// set the "allow_mult" property
    pub fn set_allow_mult(&mut self, allow_mult: bool) {
        self.0.allow_mult = Some(allow_mult);
    }

    /// get the "backend" property
    pub fn get_backend(&self) -> Option<&[u8]> {
        self.0.backend.as_deref()
    }

    /// set the "backend" property
    pub fn set_backend<T: Into<Vec<u8>>>(&mut self, backend: T) {
        self.0.backend = Some(backend.into());
    }

    /// get the "basic_quorum" property
    pub fn get_basic_quorum(&self) -> Option<bool> {
        self.0.basic_quorum
    }

    /// set the "basic_quorum" property
    pub fn set_basic_quorum(&mut self, basic_quorum: bool) {
        self.0.basic_quorum = Some(basic_quorum);
    }

    /// get the "big_vclock" property
    pub fn get_big_vclock(&self) -> Option<u32> {
        self.0.big_vclock
    }

    /// set the "big vclock" property
    pub fn set_big_vclock(&mut self, big_vclock: u32) {
        self.0.big_vclock = Some(big_vclock);
    }

    /// get the "chash_keyfun" property
    pub fn get_chash_keyfun(&self) -> Option<(&[u8], &[u8])> {
        self.0.chash_keyfun.as_ref().map(Into::into)
    }

    /// set the "chash_keyfun" property
    pub fn set_chash_keyfun<T: Into<Vec<u8>>>(&mut self, module: T, function: T) {
        self.0.chash_keyfun = Some((module, function).into());
    }

    /// get the "consistent" property
    pub fn get_consistent(&self) -> Option<bool> {
        self.0.consistent
    }

    /// set the "consistent" property
    pub fn set_consistent(&mut self, consistent: bool) {
        self.0.consistent = Some(consistent);
    }

    /// get the "datatype" property
    pub fn get_datatype(&self) -> Option<&[u8]> {
        self.0.datatype.as_deref()
    }

    /// set the "datatype" property
    pub fn set_datatype<T: Into<Vec<u8>>>(&mut self, datatype: T) {
        self.0.datatype = Some(datatype.into());
    }

    /// get the "dw" property
    pub fn get_dw(&self) -> Option<u32> {
        self.0.dw
    }

    /// set the "dw" property
    pub fn set_dw(&mut self, dw: u32) {
        self.0.dw = Some(dw);
    }

    /// get the "has_postcommit" property
    pub fn get_has_postcommit(&self) -> Option<bool> {
        self.0.has_postcommit
    }

    /// set the "has_postcommit" property
    pub fn set_has_postcommit(&mut self, has_postcommit: bool) {
        self.0.has_postcommit = Some(has_postcommit);
    }

    /// get the "has_precommit" property
    pub fn get_has_precommit(&self) -> Option<bool> {
        self.0.has_precommit
    }

    /// set the "has_precommit" property
    pub fn set_has_precommit(&mut self, has_precommit: bool) {
        self.0.has_precommit = Some(has_precommit);
    }

    /// get the "hll_precision" property
    pub fn get_hll_precision(&self) -> Option<u32> {
        self.0.hll_precision
    }

    /// set the "hll_precision" property
    pub fn set_hll_precision(&mut self, hll_precision: u32) {
        self.0.hll_precision = Some(hll_precision);
    }

    /// get the "last_write_wins" property
    pub fn get_last_write_wins(&self) -> Option<bool> {
        self.0.last_write_wins
    }

    /// set the "last_write_wins" property
    pub fn set_last_write_wins(&mut self, last_write_wins: bool) {
        self.0.last_write_wins = Some(last_write_wins);
    }

    /// get the "linkfun" property
    pub fn get_linkfun(&self) -> Option<(&[u8], &[u8])> {
        self.0.linkfun.as_ref().map(Into::into)
    }

    /// set the "linkfun" property
    pub fn set_linkfun<T: Into<Vec<u8>>>(&mut self, module: T, function: T) {
        self.0.linkfun = Some((module, function).into());
    }

    /// get the "notfound_ok" property
    pub fn get_notfound_ok(&self) -> Option<bool> {
        self.0.notfound_ok
    }

    /// set the "notfound_ok" property
    pub fn set_notfound_ok(&mut self, notfound_ok: bool) {
        self.0.notfound_ok = Some(notfound_ok);
    }

    /// get the "n_val" property
    pub fn get_n_val(&self) -> Option<u32> {
        self.0.n_val
    }

    /// set the "n_val" property
    pub fn set_n_val(&mut self, n_val: u32) {
        self.0.n_val = Some(n_val);
    }

    /// get the "old_vclock" property
    pub fn get_old_vclock(&self) -> Option<u32> {
        self.0.old_vclock
    }

    /// set the "old_vclock" property
    pub fn set_old_vclock(&mut self, old_vclock: u32) {
        self.0.old_vclock = Some(old_vclock);
    }

    /// get the "postcommit" property
    pub fn get_postcommit(&self) -> Option<CommitHooks> {
        CommitHooks::from_proto(&self.0.postcommit)
    }

    /// set the "postcommit" property
    pub fn set_postcommit(&mut self, postcommit: CommitHooks) {
        self.0.postcommit = postcommit.into();
    }

    /// get the "precommit" property
    pub fn get_precommit(&self) -> Option<CommitHooks> {
        CommitHooks::from_proto(&self.0.precommit)
    }

    /// set the "precommit" property
    pub fn set_precommit(&mut self, precommit: CommitHooks) {
        self.0.precommit = precommit.into();
    }

    /// get the "pr" property
    pub fn get_pr(&self) -> Option<u32> {
        self.0.pr
    }

    /// set the "pr" property
    pub fn set_pr(&mut self, pr: u32) {
        self.0.pr = Some(pr);
    }

    /// get the "pw" property
    pub fn get_pw(&self) -> Option<u32> {
        self.0.pw
    }

    /// set the "pw" property
    pub fn set_pw(&mut self, pw: u32) {
        self.0.pw = Some(pw);
    }

    /// get the "r" property
    pub fn get_r(&self) -> Option<u32> {
        self.0.r
    }

    /// set the "r" property
    pub fn set_r(&mut self, r: u32) {
        self.0.r = Some(r);
    }

    /// get the "rw" property
    pub fn get_rw(&self) -> Option<u32> {
        self.0.rw
    }

    /// set the "rw" property
    pub fn set_rw(&mut self, rw: u32) {
        self.0.rw = Some(rw);
    }

    /// get the "search_index" property
    pub fn get_search_index(&self) -> Option<&[u8]> {
        self.0.search_index.as_deref()
    }

    /// set the "search_index" property
    pub fn set_search_index<T: Into<Vec<u8>>>(&mut self, search_index: T) {
        self.0.search_index = Some(search_index.into());
    }

    /// get the "search" property
    pub fn get_search(&self) -> Option<bool> {
        self.0.search
    }

    /// set the "search" property
    pub fn set_search(&mut self, search: bool) {
        self.0.search = Some(search);
    }

    /// get the "small_vclock" property
    pub fn get_small_vclock(&self) -> Option<u32> {
        self.0.small_vclock
    }

    /// set the "small_vclock" property
    pub fn set_small_vclock(&mut self, small_vclock: u32) {
        self.0.small_vclock = Some(small_vclock);
    }

    /// get the "write_once" property
    pub fn get_write_once(&self) -> Option<bool> {
        self.0.write_once
    }

    /// set the "write_once" property
    pub fn set_write_once(&mut self, write_once: bool) {
        self.0.write_once = Some(write_once);
    }

    /// get the "w" property
    pub fn get_w(&self) -> Option<u32> {
        self.0.w
    }

    /// set the "w" property
    pub fn set_w(&mut self, w: u32) {
        self.0.w = Some(w);
    }

    /// get the "young_vclock" property
    pub fn get_young_vclock(&self) -> Option<u32> {
        self.0.young_vclock
    }

    /// set the "young_vclock" property
    pub fn set_young_vclock(&mut self, young_vclock: u32) {
        self.0.young_vclock = Some(young_vclock);
    }

    pub fn serialize(self) -> Result<Vec<u8>, RiakErr> {
        let req = RpbSetBucketReq {
            props: self.0,
            bucket: self.1,
            ..Default::default()
        };
        Ok(req.encode_to_vec())
    }
}

/// `BucketTypeProps` represents the properties that can bet set on a bucket type
#[derive(Clone, Debug)]
pub struct BucketTypeProps(RpbBucketProps, Vec<u8>);

impl BucketTypeProps {
    /// constructs a new `BucketTypeProps`
    pub fn new<T: Into<Vec<u8>>>(bucket_type_name: T) -> BucketTypeProps {
        BucketTypeProps(RpbBucketProps::default(), bucket_type_name.into())
    }

    pub fn new_with_props<T: Into<Vec<u8>>>(
        bucket_type_name: T,
        props: RpbBucketProps,
    ) -> BucketTypeProps {
        BucketTypeProps(props, bucket_type_name.into())
    }

    /// get the "bucket" property
    pub fn get_bucket(&self) -> &[u8] {
        self.1.as_slice()
    }

    /// set the "bucket" property
    pub fn set_bucket<T: Into<Vec<u8>>>(&mut self, bucket: T) {
        self.1 = bucket.into();
    }

    /// get the "allow_mult" property
    pub fn get_allow_mult(&self) -> Option<bool> {
        self.0.allow_mult
    }

    /// set the "allow_mult" property
    pub fn set_allow_mult(&mut self, allow_mult: bool) {
        self.0.allow_mult = Some(allow_mult);
    }

    /// get the "backend" property
    pub fn get_backend(&self) -> Option<&[u8]> {
        self.0.backend.as_deref()
    }

    /// set the "backend" property
    pub fn set_backend<T: Into<Vec<u8>>>(&mut self, backend: T) {
        self.0.backend = Some(backend.into());
    }

    /// get the "basic_quorum" property
    pub fn get_basic_quorum(&self) -> Option<bool> {
        self.0.basic_quorum
    }

    /// set the "basic_quorum" property
    pub fn set_basic_quorum(&mut self, basic_quorum: bool) {
        self.0.basic_quorum = Some(basic_quorum);
    }

    /// get the "big_vclock" property
    pub fn get_big_vclock(&self) -> Option<u32> {
        self.0.big_vclock
    }

    /// set the "big vclock" property
    pub fn set_big_vclock(&mut self, big_vclock: u32) {
        self.0.big_vclock = Some(big_vclock);
    }

    /// get the "chash_keyfun" property
    pub fn get_chash_keyfun(&self) -> Option<(&[u8], &[u8])> {
        self.0.chash_keyfun.as_ref().map(Into::into)
    }

    /// set the "chash_keyfun" property
    pub fn set_chash_keyfun<T: Into<Vec<u8>>>(&mut self, module: T, function: T) {
        self.0.chash_keyfun = Some((module, function).into());
    }

    /// get the "consistent" property
    pub fn get_consistent(&self) -> Option<bool> {
        self.0.consistent
    }

    /// set the "consistent" property
    pub fn set_consistent(&mut self, consistent: bool) {
        self.0.consistent = Some(consistent);
    }

    /// get the "datatype" property
    pub fn get_datatype(&self) -> Option<&[u8]> {
        self.0.datatype.as_deref()
    }

    /// set the "datatype" property
    pub fn set_datatype<T: Into<Vec<u8>>>(&mut self, datatype: T) {
        self.0.datatype = Some(datatype.into());
    }

    /// get the "dw" property
    pub fn get_dw(&self) -> Option<u32> {
        self.0.dw
    }

    /// set the "dw" property
    pub fn set_dw(&mut self, dw: u32) {
        self.0.dw = Some(dw);
    }

    /// get the "has_postcommit" property
    pub fn get_has_postcommit(&self) -> Option<bool> {
        self.0.has_postcommit
    }

    /// set the "has_postcommit" property
    pub fn set_has_postcommit(&mut self, has_postcommit: bool) {
        self.0.has_postcommit = Some(has_postcommit);
    }

    /// get the "has_precommit" property
    pub fn get_has_precommit(&self) -> Option<bool> {
        self.0.has_precommit
    }

    /// set the "has_precommit" property
    pub fn set_has_precommit(&mut self, has_precommit: bool) {
        self.0.has_precommit = Some(has_precommit);
    }

    /// get the "hll_precision" property
    pub fn get_hll_precision(&self) -> Option<u32> {
        self.0.hll_precision
    }

    /// set the "hll_precision" property
    pub fn set_hll_precision(&mut self, hll_precision: u32) {
        self.0.hll_precision = Some(hll_precision);
    }

    /// get the "last_write_wins" property
    pub fn get_last_write_wins(&self) -> Option<bool> {
        self.0.last_write_wins
    }

    /// set the "last_write_wins" property
    pub fn set_last_write_wins(&mut self, last_write_wins: bool) {
        self.0.last_write_wins = Some(last_write_wins);
    }

    /// get the "linkfun" property
    pub fn get_linkfun(&self) -> Option<(&[u8], &[u8])> {
        self.0.linkfun.as_ref().map(Into::into)
    }

    /// set the "linkfun" property
    pub fn set_linkfun<T: Into<Vec<u8>>>(&mut self, module: T, function: T) {
        self.0.linkfun = Some((module, function).into());
    }

    /// get the "notfound_ok" property
    pub fn get_notfound_ok(&self) -> Option<bool> {
        self.0.notfound_ok
    }

    /// set the "notfound_ok" property
    pub fn set_notfound_ok(&mut self, notfound_ok: bool) {
        self.0.notfound_ok = Some(notfound_ok);
    }

    /// get the "n_val" property
    pub fn get_n_val(&self) -> Option<u32> {
        self.0.n_val
    }

    /// set the "n_val" property
    pub fn set_n_val(&mut self, n_val: u32) {
        self.0.n_val = Some(n_val);
    }

    /// get the "old_vclock" property
    pub fn get_old_vclock(&self) -> Option<u32> {
        self.0.old_vclock
    }

    /// set the "old_vclock" property
    pub fn set_old_vclock(&mut self, old_vclock: u32) {
        self.0.old_vclock = Some(old_vclock);
    }

    /// get the "postcommit" property
    pub fn get_postcommit(&self) -> Option<CommitHooks> {
        CommitHooks::from_proto(&self.0.postcommit)
    }

    /// set the "postcommit" property
    pub fn set_postcommit(&mut self, postcommit: CommitHooks) {
        self.0.postcommit = postcommit.into();
    }

    /// get the "precommit" property
    pub fn get_precommit(&self) -> Option<CommitHooks> {
        CommitHooks::from_proto(&self.0.precommit)
    }

    /// set the "precommit" property
    pub fn set_precommit(&mut self, precommit: CommitHooks) {
        self.0.precommit = precommit.into();
    }

    /// get the "pr" property
    pub fn get_pr(&self) -> Option<u32> {
        self.0.pr
    }

    /// set the "pr" property
    pub fn set_pr(&mut self, pr: u32) {
        self.0.pr = Some(pr);
    }

    /// get the "pw" property
    pub fn get_pw(&self) -> Option<u32> {
        self.0.pw
    }

    /// set the "pw" property
    pub fn set_pw(&mut self, pw: u32) {
        self.0.pw = Some(pw);
    }

    /// get the "r" property
    pub fn get_r(&self) -> Option<u32> {
        self.0.r
    }

    /// set the "r" property
    pub fn set_r(&mut self, r: u32) {
        self.0.r = Some(r);
    }

    /// get the "rw" property
    pub fn get_rw(&self) -> Option<u32> {
        self.0.rw
    }

    /// set the "rw" property
    pub fn set_rw(&mut self, rw: u32) {
        self.0.rw = Some(rw);
    }

    /// get the "search_index" property
    pub fn get_search_index(&self) -> Option<&[u8]> {
        self.0.search_index.as_deref()
    }

    /// set the "search_index" property
    pub fn set_search_index<T: Into<Vec<u8>>>(&mut self, search_index: T) {
        self.0.search_index = Some(search_index.into());
    }

    /// get the "search" property
    pub fn get_search(&self) -> Option<bool> {
        self.0.search
    }

    /// set the "search" property
    pub fn set_search(&mut self, search: bool) {
        self.0.search = Some(search);
    }

    /// get the "small_vclock" property
    pub fn get_small_vclock(&self) -> Option<u32> {
        self.0.small_vclock
    }

    /// set the "small_vclock" property
    pub fn set_small_vclock(&mut self, small_vclock: u32) {
        self.0.small_vclock = Some(small_vclock);
    }

    /// get the "write_once" property
    pub fn get_write_once(&self) -> Option<bool> {
        self.0.write_once
    }

    /// set the "write_once" property
    pub fn set_write_once(&mut self, write_once: bool) {
        self.0.write_once = Some(write_once);
    }

    /// get the "w" property
    pub fn get_w(&self) -> Option<u32> {
        self.0.w
    }

    /// set the "w" property
    pub fn set_w(&mut self, w: u32) {
        self.0.w = Some(w);
    }

    /// get the "young_vclock" property
    pub fn get_young_vclock(&self) -> Option<u32> {
        self.0.young_vclock
    }

    /// set the "young_vclock" property
    pub fn set_young_vclock(&mut self, young_vclock: u32) {
        self.0.young_vclock = Some(young_vclock);
    }

    pub fn serialize(self) -> Result<Vec<u8>, RiakErr> {
        let req = RpbSetBucketTypeReq {
            r#type: self.1,
            props: self.0,
        };
        Ok(req.encode_to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::{BucketProps, BucketTypeProps};

    #[test]
    fn bucket_props() {
        // get_bucket() & set_bucket()
        let mut props = BucketProps::new("test_bucket");
        assert_eq!(b"test_bucket", props.get_bucket());
        props.set_bucket("test_bucket_two");
        assert_eq!(b"test_bucket_two", props.get_bucket());
    }

    #[test]
    fn bucket_type_props() {
        let mut props = BucketTypeProps::new("test_bucket_type");
        assert_eq!(b"test_bucket_type", props.get_bucket());
        props.set_bucket("test_bucket_type_two");
        assert_eq!(b"test_bucket_type_two", props.get_bucket());
    }
}
