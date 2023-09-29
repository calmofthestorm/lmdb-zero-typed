use lmdb_zero::traits::{AsLmdbBytes, FromLmdbBytes, FromReservedLmdbBytes};
use lmdb_zero::Result;

use crate::{Database, Layout};

#[derive(Debug)]
pub enum ConstAccessor<'txn> {
    Read(lmdb_zero::ConstAccessor<'txn>),
    Write(lmdb_zero::WriteAccessor<'txn>),
}

#[derive(Debug)]
pub struct WriteAccessor<'env>(ConstAccessor<'env>);

impl<'txn> ConstAccessor<'txn> {
    pub fn get<'env, K, V, L>(&self, db: &Database<'env, K, V, L>, key: &K) -> Result<&V>
    where
        K: AsLmdbBytes + ?Sized,
        V: FromLmdbBytes + ?Sized,
        'env: 'txn,
        L: Layout,
    {
        match self {
            ConstAccessor::Read(access) => access.get(&db.0, key),
            ConstAccessor::Write(access) => access.get(&db.0, key),
        }
    }

    pub fn as_lmdb(&self) -> &lmdb_zero::ConstAccessor<'txn> {
        match self {
            ConstAccessor::Read(access) => access,
            ConstAccessor::Write(access) => access,
        }
    }
}

impl<'txn> WriteAccessor<'txn> {
    #[inline]
    pub fn put<K, V, L>(
        &mut self,
        db: &Database<K, V, L>,
        key: &K,
        value: &V,
        flags: lmdb_zero::put::Flags,
    ) -> Result<()>
    where
        K: AsLmdbBytes + ?Sized,
        V: AsLmdbBytes + ?Sized,
        L: Layout,
    {
        self.as_lmdb_mut().put(&db.0, key, value, flags)
    }

    #[inline]
    pub fn from_lmdb(access: lmdb_zero::WriteAccessor<'txn>) -> WriteAccessor<'txn> {
        Self(ConstAccessor::Write(access))
    }

    #[inline]
    pub fn put_reserve<K, V, L>(
        &mut self,
        db: &Database<K, V, L>,
        key: &K,
        flags: lmdb_zero::put::Flags,
    ) -> Result<&mut V>
    where
        K: AsLmdbBytes + ?Sized,
        V: FromReservedLmdbBytes + Sized,
        L: Layout,
    {
        self.as_lmdb_mut().put_reserve(&db.0, key, flags)
    }

    #[inline]
    pub unsafe fn put_reserve_unsized<K, V, L>(
        &mut self,
        db: &Database<K, V, L>,
        key: &K,
        size: usize,
        flags: lmdb_zero::put::Flags,
    ) -> Result<&mut V>
    where
        K: AsLmdbBytes + ?Sized,
        V: FromReservedLmdbBytes + ?Sized,
        L: Layout,
    {
        self.as_lmdb_mut()
            .put_reserve_unsized(&db.0, key, size, flags)
    }

    #[inline]
    pub fn del_key<K, V, L>(&mut self, db: &Database<K, V, L>, key: &K) -> Result<()>
    where
        K: AsLmdbBytes + ?Sized,
        L: Layout,
    {
        self.as_lmdb_mut().del_key(&db.0, key)
    }

    #[inline]
    pub fn del_item<K, V, L>(&mut self, db: &Database<K, V, L>, key: &K, val: &V) -> Result<()>
    where
        K: AsLmdbBytes + ?Sized,
        V: AsLmdbBytes + ?Sized,
        L: Layout,
    {
        self.as_lmdb_mut().del_item(&db.0, key, val)
    }

    #[inline]
    pub fn clear_db<K, V, L>(&mut self, db: &Database<K, V, L>) -> Result<()>
    where
        L: Layout,
    {
        self.as_lmdb_mut().clear_db(&db.0)
    }

    #[inline]
    pub fn as_lmdb_mut(&mut self) -> &mut lmdb_zero::WriteAccessor<'txn> {
        match &mut self.0 {
            ConstAccessor::Write(access) => access,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn as_lmdb(&self) -> &lmdb_zero::WriteAccessor<'txn> {
        match &self.0 {
            ConstAccessor::Write(access) => access,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn into_lmdb(self) -> lmdb_zero::WriteAccessor<'txn> {
        match self.0 {
            ConstAccessor::Write(access) => access,
            _ => unreachable!(),
        }
    }
}

impl<'txn> std::ops::Deref for WriteAccessor<'txn> {
    type Target = ConstAccessor<'txn>;

    fn deref(&self) -> &ConstAccessor<'txn> {
        &self.0
    }
}
