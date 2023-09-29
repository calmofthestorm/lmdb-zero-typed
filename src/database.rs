use lmdb_zero::{Environment, Result};

use supercow::Supercow;

use crate::Layout;

#[derive(Debug)]
pub struct Database<'e, K: ?Sized, V: ?Sized, L: Layout>(
    pub lmdb_zero::Database<'e>,
    std::marker::PhantomData<K>,
    std::marker::PhantomData<V>,
    std::marker::PhantomData<L>,
);

impl<'e, K: ?Sized, V: ?Sized, L: Layout> Database<'e, K, V, L> {
    #[inline]
    pub fn from_lmdb<E>(db: lmdb_zero::Database<'e>) -> Self {
        Database(
            db,
            std::marker::PhantomData,
            std::marker::PhantomData,
            std::marker::PhantomData,
        )
    }

    #[inline]
    pub fn open<E>(
        env: E,
        name: Option<&str>,
        options: &lmdb_zero::DatabaseOptions,
    ) -> Result<Database<'e, K, V, L>>
    where
        E: Into<Supercow<'e, Environment>>,
    {
        Ok(Database::from_lmdb::<E>(lmdb_zero::Database::open(
            env, name, options,
        )?))
    }

    #[inline]
    pub fn delete(self) -> Result<()> {
        self.0.delete()
    }

    #[inline]
    pub fn env(&self) -> &Environment {
        self.0.env()
    }

    #[inline]
    pub fn as_lmdb(&self) -> &lmdb_zero::Database<'e> {
        &self.0
    }

    #[inline]
    pub fn as_lmdb_mut(&mut self) -> &mut lmdb_zero::Database<'e> {
        &mut self.0
    }

    #[inline]
    pub fn into_lmdb(self) -> lmdb_zero::Database<'e> {
        self.0
    }
}
