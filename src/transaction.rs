use lmdb_zero::{Environment, Result};
use supercow::NonSyncSupercow;

use crate::{ConstAccessor, Cursor, Database, Layout, StaleCursor, WriteAccessor};

#[derive(Debug)]
pub struct ResetTransaction<'env>(pub lmdb_zero::ResetTransaction<'env>);

#[derive(Debug)]
pub enum ConstTransaction<'env> {
    Read(lmdb_zero::ReadTransaction<'env>),
    Write(lmdb_zero::WriteTransaction<'env>),
}

#[derive(Debug)]
pub struct ReadTransaction<'env>(ConstTransaction<'env>);

#[derive(Debug)]
pub struct WriteTransaction<'env>(ConstTransaction<'env>);

impl<'env> ConstTransaction<'env> {
    #[inline]
    pub fn cursor<'txn, 'db, K, V, L>(
        &'txn self,
        db: &'db Database<'env, K, V, L>,
    ) -> Result<Cursor<'txn, 'db, K, V, L>>
    where
        K: 'db + ?Sized,
        V: 'db + ?Sized,
        L: Layout,
        'env: 'db,
    {
        match self {
            ConstTransaction::Read(txn) => txn.cursor(&db.0),
            ConstTransaction::Write(txn) => txn.cursor(&db.0),
        }
        .map(Cursor::from_lmdb)
    }

    #[inline]
    pub fn access<'txn>(&'txn self) -> ConstAccessor<'txn> {
        match self {
            ConstTransaction::Read(txn) => ConstAccessor::Read(txn.access()),
            ConstTransaction::Write(txn) => ConstAccessor::Write(txn.access()),
        }
    }

    #[inline]
    pub fn id(&self) -> usize {
        match self {
            ConstTransaction::Read(txn) => txn.id(),
            ConstTransaction::Write(txn) => txn.id(),
        }
    }

    #[inline]
    pub fn db_stat<K, V, L: Layout>(&self, db: &Database<K, V, L>) -> Result<lmdb_zero::Stat> {
        match self {
            ConstTransaction::Read(txn) => txn.db_stat(&db.0),
            ConstTransaction::Write(txn) => txn.db_stat(&db.0),
        }
    }

    #[inline]
    pub fn db_flags<K, V, L: Layout>(
        &self,
        db: &Database<K, V, L>,
    ) -> Result<lmdb_zero::db::Flags> {
        match self {
            ConstTransaction::Read(txn) => txn.db_flags(&db.0),
            ConstTransaction::Write(txn) => txn.db_flags(&db.0),
        }
    }

    #[inline]
    pub fn as_lmdb(&self) -> &lmdb_zero::ConstTransaction<'env> {
        match &self {
            ConstTransaction::Write(txn) => &*txn,
            ConstTransaction::Read(txn) => &*txn,
        }
    }
}

impl<'env> ReadTransaction<'env> {
    #[inline]
    pub fn from_lmdb(inner: lmdb_zero::ReadTransaction<'env>) -> ReadTransaction<'env> {
        Self(ConstTransaction::Read(inner))
    }

    #[inline]
    pub fn new<E>(env: E) -> Result<Self>
    where
        E: Into<NonSyncSupercow<'env, Environment>>,
    {
        lmdb_zero::ReadTransaction::new(env).map(Self::from_lmdb)
    }

    #[inline]
    pub fn access(&self) -> ConstAccessor {
        self.0.access()
    }

    #[inline]
    pub fn cursor<'txn, 'db, K, V, L>(
        &'txn self,
        db: &'db Database<'env, K, V, L>,
    ) -> Result<Cursor<'txn, 'db, K, V, L>>
    where
        K: 'db + ?Sized,
        V: 'db + ?Sized,
        L: Layout,
        'env: 'db,
    {
        self.as_lmdb().cursor(&db.0).map(Cursor::from_lmdb)
    }

    #[inline]
    pub fn dissoc_cursor<'txn, 'db, K, V, L: Layout>(
        &self,
        cursor: Cursor<'txn, 'db, K, V, L>,
    ) -> Result<StaleCursor<'db, K, V, L>>
    where
        'env: 'db,
    {
        self.as_lmdb()
            .dissoc_cursor(cursor.0)
            .map(StaleCursor::from_lmdb)
    }

    #[inline]
    pub fn assoc_cursor<'txn, 'db, K, V, L: Layout>(
        &'txn self,
        cursor: StaleCursor<'db, K, V, L>,
    ) -> Result<Cursor<'txn, 'db, K, V, L>> {
        self.as_lmdb().assoc_cursor(cursor.0).map(Cursor::from_lmdb)
    }

    #[inline]
    pub fn reset(self) -> ResetTransaction<'env> {
        ResetTransaction(self.into_lmdb().reset())
    }

    #[inline]
    pub fn as_lmdb(&self) -> &lmdb_zero::ReadTransaction<'env> {
        match &self.0 {
            ConstTransaction::Read(txn) => txn,
            _ => unreachable!(),
        }
    }

    #[inline]
    pub fn into_lmdb(self) -> lmdb_zero::ReadTransaction<'env> {
        match self.0 {
            ConstTransaction::Read(txn) => txn,
            _ => unreachable!(),
        }
    }
}

impl<'txn> std::ops::Deref for ReadTransaction<'txn> {
    type Target = ConstTransaction<'txn>;

    fn deref(&self) -> &ConstTransaction<'txn> {
        &self.0
    }
}

impl<'env> ResetTransaction<'env> {
    #[inline]
    pub fn from_lmdb(inner: lmdb_zero::ResetTransaction<'env>) -> ResetTransaction<'env> {
        Self(inner)
    }

    #[inline]
    pub fn renew(self) -> Result<ReadTransaction<'env>> {
        self.0.renew().map(ReadTransaction::from_lmdb)
    }

    #[inline]
    pub fn as_lmdb(&self) -> &lmdb_zero::ResetTransaction<'env> {
        &self.0
    }

    #[inline]
    pub fn as_lmdb_mut(&mut self) -> &mut lmdb_zero::ResetTransaction<'env> {
        &mut self.0
    }

    #[inline]
    pub fn into_lmdb(self) -> lmdb_zero::ResetTransaction<'env> {
        self.0
    }
}

impl<'env> WriteTransaction<'env> {
    #[inline]
    pub fn from_lmdb(inner: lmdb_zero::WriteTransaction<'env>) -> WriteTransaction<'env> {
        Self(ConstTransaction::Write(inner))
    }

    #[inline]
    pub fn new<E>(env: E) -> Result<Self>
    where
        E: Into<NonSyncSupercow<'env, Environment>>,
    {
        lmdb_zero::WriteTransaction::new(env).map(WriteTransaction::from_lmdb)
    }

    #[inline]
    pub fn access(&self) -> WriteAccessor {
        WriteAccessor::from_lmdb(self.as_lmdb().access())
    }

    #[inline]
    pub fn cursor<'txn, 'db, K, V, L>(
        &'txn self,
        db: &'db Database<'env, K, V, L>,
    ) -> Result<Cursor<'txn, 'db, K, V, L>>
    where
        K: 'db + ?Sized,
        V: 'db + ?Sized,
        L: Layout,
        'env: 'db,
    {
        self.as_lmdb().cursor(&db.0).map(Cursor::from_lmdb)
    }

    #[inline]
    pub fn child_tx<'a>(&'a mut self) -> Result<WriteTransaction<'a>>
    where
        'env: 'a,
    {
        self.as_lmdb_mut()
            .child_tx()
            .map(WriteTransaction::from_lmdb)
    }

    #[inline]
    pub fn commit(self) -> Result<()> {
        self.into_lmdb().commit()
    }

    #[inline]
    fn as_lmdb(&self) -> &lmdb_zero::WriteTransaction<'env> {
        match &self.0 {
            ConstTransaction::Write(txn) => txn,
            _ => unreachable!(),
        }
    }

    #[inline]
    fn as_lmdb_mut(&mut self) -> &mut lmdb_zero::WriteTransaction<'env> {
        match &mut self.0 {
            ConstTransaction::Write(txn) => txn,
            _ => unreachable!(),
        }
    }

    #[inline]
    fn into_lmdb(self) -> lmdb_zero::WriteTransaction<'env> {
        match self.0 {
            ConstTransaction::Write(txn) => txn,
            _ => unreachable!(),
        }
    }
}

impl<'txn> std::ops::Deref for WriteTransaction<'txn> {
    type Target = ConstTransaction<'txn>;

    fn deref(&self) -> &ConstTransaction<'txn> {
        &self.0
    }
}
