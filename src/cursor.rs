use lmdb_zero::{
    traits::{AsLmdbBytes, FromLmdbBytes, FromReservedLmdbBytes, LmdbRaw},
    Result,
};

use crate::{
    ConstAccessor, Layout, LayoutDupfixed, LayoutDupsort, LayoutNoDuplicates, WriteAccessor,
};

pub struct Cursor<'t, 'd, K: ?Sized, V: ?Sized, L: Layout>(
    pub lmdb_zero::Cursor<'t, 'd>,
    std::marker::PhantomData<K>,
    std::marker::PhantomData<V>,
    std::marker::PhantomData<L>,
);

#[derive(Debug)]
pub struct StaleCursor<'d, K: ?Sized, V: ?Sized, L: Layout>(
    pub lmdb_zero::StaleCursor<'d>,
    pub(crate) std::marker::PhantomData<K>,
    pub(crate) std::marker::PhantomData<V>,
    pub(crate) std::marker::PhantomData<L>,
);

macro_rules! t_get_0_kv {
    ($method:ident) => {
        fn $method<'access>(
            &mut self,
            access: &'access ConstAccessor<'t>,
        ) -> Result<(&'access K, &'access V)>;
    };
}

macro_rules! c_get_0_kv {
    ($method:ident) => {
        #[inline]
        fn $method<'access>(
            &mut self,
            access: &'access ConstAccessor<'t>,
        ) -> Result<(&'access K, &'access V)> {
            self.0.$method(&access.as_lmdb())
        }
    };
}

macro_rules! t_get_0_v {
    ($method:ident) => {
        fn $method<'access>(&mut self, access: &'access ConstAccessor<'t>) -> Result<&'access V>;
    };
}

macro_rules! c_get_0_v {
    ($method:ident) => {
        #[inline]
        fn $method<'access>(&mut self, access: &'access ConstAccessor<'t>) -> Result<&'access V> {
            self.0.$method(&access.as_lmdb())
        }
    };
}

macro_rules! t_change_in_place {
    ($method:ident) => {
        fn $method<'access>(
            &mut self,
            access: &'access mut WriteAccessor,
            key: &K,
            flags: lmdb_zero::put::Flags,
        ) -> Result<&'access mut V>;
    };
}

macro_rules! c_change_in_place {
    ($method:ident) => {
        #[inline]
        fn $method<'access>(
            &mut self,
            access: &'access mut WriteAccessor,
            key: &K,
            flags: lmdb_zero::put::Flags,
        ) -> Result<&'access mut V> {
            self.0.$method(access.as_lmdb_mut(), key, flags)
        }
    };
}

macro_rules! t_change_in_place_unsized {
    ($method:ident) => {
        unsafe fn $method<'access>(
            &mut self,
            access: &'access mut WriteAccessor,
            key: &K,
            size: usize,
            flags: lmdb_zero::put::Flags,
        ) -> Result<&'access mut V>;
    };
}

macro_rules! c_change_in_place_unsized {
    ($method:ident) => {
        #[inline]
        unsafe fn $method<'access>(
            &mut self,
            access: &'access mut WriteAccessor,
            key: &K,
            size: usize,
            flags: lmdb_zero::put::Flags,
        ) -> Result<&'access mut V> {
            self.0.$method(access.as_lmdb_mut(), key, size, flags)
        }
    };
}

macro_rules! t_put {
    ($method:ident, $value_type:ty, $result_type:ty) => {
        fn $method(
            &mut self,
            access: &mut WriteAccessor,
            key: &K,
            value: &$value_type,
            flags: lmdb_zero::put::Flags,
        ) -> Result<$result_type>;
    };
}

macro_rules! c_put {
    ($method:ident, $value_type:ty, $result_type:ty) => {
        #[inline]
        fn $method(
            &mut self,
            access: &mut WriteAccessor,
            key: &K,
            value: &$value_type,
            flags: lmdb_zero::put::Flags,
        ) -> Result<$result_type> {
            self.0.$method(access.as_lmdb_mut(), key, value, flags)
        }
    };
}

macro_rules! t_seek_both {
    ($method:ident) => {
        fn $method<'access>(
            &mut self,
            access: &'access ConstAccessor<'t>,
            key: &K,
        ) -> Result<(&'access K, &'access V)>;
    };
}

macro_rules! c_seek_both {
    ($method:ident) => {
        #[inline]
        fn $method<'access>(
            &mut self,
            access: &'access ConstAccessor<'t>,
            key: &K,
        ) -> Result<(&'access K, &'access V)> {
            self.0.$method(&access.as_lmdb(), key)
        }
    };
}

impl<'t, 'd, K, V, L> Cursor<'t, 'd, K, V, L>
where
    K: ?Sized,
    V: ?Sized,
    L: Layout,
{
    #[inline]
    pub fn from_lmdb(cursor: lmdb_zero::Cursor<'t, 'd>) -> Cursor<'t, 'd, K, V, L> {
        Cursor(
            cursor,
            std::marker::PhantomData,
            std::marker::PhantomData,
            std::marker::PhantomData,
        )
    }

    #[inline]
    pub fn del(&mut self, access: &mut WriteAccessor, flags: lmdb_zero::del::Flags) -> Result<()> {
        self.0.del(access.as_lmdb_mut(), flags)
    }

    #[inline]
    pub fn as_lmdb(&self) -> &lmdb_zero::Cursor<'t, 'd> {
        &self.0
    }

    #[inline]
    pub fn as_lmdb_mut(&mut self) -> &mut lmdb_zero::Cursor<'t, 'd> {
        &mut self.0
    }

    #[inline]
    pub fn into_lmdb(self) -> lmdb_zero::Cursor<'t, 'd> {
        self.0
    }
}

impl<'d, K, V, L: Layout> StaleCursor<'d, K, V, L> {
    #[inline]
    pub fn from_lmdb(cursor: lmdb_zero::StaleCursor<'d>) -> StaleCursor<'d, K, V, L> {
        StaleCursor(
            cursor,
            std::marker::PhantomData,
            std::marker::PhantomData,
            std::marker::PhantomData,
        )
    }

    #[inline]
    pub fn as_lmdb(&self) -> &lmdb_zero::StaleCursor<'d> {
        &self.0
    }

    #[inline]
    pub fn as_lmdb_mut(&mut self) -> &mut lmdb_zero::StaleCursor<'d> {
        &mut self.0
    }

    #[inline]
    pub fn into_lmdb(self) -> lmdb_zero::StaleCursor<'d> {
        self.0
    }
}

pub trait CursorDupsort<'t, 'd, K, V, L>
where
    L: Layout + LayoutDupsort,
{
    fn count(&mut self) -> Result<usize>;
}

impl<'t, 'd, K, V, L> CursorDupsort<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    L: Layout + LayoutDupsort,
{
    #[inline]
    fn count(&mut self) -> Result<usize> {
        self.0.count()
    }
}

pub trait CursorFromXFrom<'t, 'd, K, V, L>
where
    K: FromLmdbBytes + ?Sized,
    V: FromLmdbBytes + ?Sized,
    L: Layout,
{
    t_get_0_kv!(first);
    t_get_0_kv!(get_current);
    t_get_0_kv!(last);
    t_get_0_kv!(next);
    t_get_0_kv!(next_nodup);
    t_get_0_kv!(prev);
    t_get_0_kv!(prev_nodup);
}

impl<'t, 'd, K, V, L> CursorFromXFrom<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: FromLmdbBytes + ?Sized,
    V: FromLmdbBytes + ?Sized,
    L: Layout,
{
    c_get_0_kv!(first);
    c_get_0_kv!(get_current);
    c_get_0_kv!(last);
    c_get_0_kv!(next);
    c_get_0_kv!(next_nodup);
    c_get_0_kv!(prev);
    c_get_0_kv!(prev_nodup);
}

pub trait CursorFromXFromDupsort<'t, 'd, K, V, L>
where
    K: FromLmdbBytes + ?Sized,
    V: FromLmdbBytes + ?Sized,
    L: Layout + LayoutDupsort,
{
    t_get_0_kv!(next_dup);
    t_get_0_kv!(prev_dup);
}

impl<'t, 'd, K, V, L> CursorFromXFromDupsort<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: FromLmdbBytes + ?Sized,
    V: FromLmdbBytes + ?Sized,
    L: Layout + LayoutDupsort,
{
    c_get_0_kv!(next_dup);
    c_get_0_kv!(prev_dup);
}

pub trait CursorXFromDupfixed<'t, 'd, K, V, L>
where
    L: Layout + LayoutDupsort + LayoutDupfixed,
    [V]: FromLmdbBytes,
{
    fn next_multiple<'access>(
        &mut self,
        access: &'access ConstAccessor<'t>,
    ) -> Result<&'access [V]>;

    fn get_multiple<'access>(&mut self, access: &'access ConstAccessor<'t>)
        -> Result<&'access [V]>;
}

impl<'t, 'd, K, V, L> CursorXFromDupfixed<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    L: Layout + LayoutDupsort + LayoutDupfixed,
    [V]: FromLmdbBytes,
{
    #[inline]
    fn get_multiple<'access>(
        &mut self,
        access: &'access ConstAccessor<'t>,
    ) -> Result<&'access [V]> {
        self.0.get_multiple::<[V]>(&access.as_lmdb())
    }

    #[inline]
    fn next_multiple<'access>(
        &mut self,
        access: &'access ConstAccessor<'t>,
    ) -> Result<&'access [V]> {
        self.0.next_multiple::<[V]>(&access.as_lmdb())
    }
}

pub trait CursorXFromDupsort<'t, 'd, K, V, L>
where
    V: FromLmdbBytes + ?Sized,
    L: LayoutDupsort + Layout,
{
    t_get_0_v!(first_dup);
    t_get_0_v!(last_dup);
}

impl<'t, 'd, K, V, L> CursorXFromDupsort<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    V: FromLmdbBytes + ?Sized,
    L: LayoutDupsort + Layout,
{
    c_get_0_v!(first_dup);
    c_get_0_v!(last_dup);
}

pub trait CursorAsXAs<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: AsLmdbBytes + ?Sized,
    L: Layout,
{
    t_put!(put, V, ());
    t_put!(overwrite, V, ());
}

impl<'t, 'd, K, V, L> CursorAsXAs<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: AsLmdbBytes + ?Sized,
    L: Layout,
{
    c_put!(put, V, ());
    c_put!(overwrite, V, ());
}

pub trait CursorAsXAsDupsort<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: AsLmdbBytes + ?Sized,
    L: Layout + LayoutDupsort,
{
    fn seek_kv(&mut self, key: &K, val: &V) -> Result<()>;
}

impl<'t, 'd, K, V, L> CursorAsXAsDupsort<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: AsLmdbBytes + ?Sized,
    L: Layout + LayoutDupsort,
{
    #[inline]
    fn seek_kv(&mut self, key: &K, val: &V) -> Result<()> {
        self.0.seek_kv(key, val)
    }
}

pub trait CursorAsXAsFromDupsort<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: AsLmdbBytes + FromLmdbBytes + ?Sized,
    L: Layout + LayoutDupsort,
{
    fn seek_k_nearest_v<'access>(
        &mut self,
        access: &'access ConstAccessor<'t>,
        key: &K,
        val: &V,
    ) -> Result<&'access V>;
}

impl<'t, 'd, K, V, L> CursorAsXAsFromDupsort<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: AsLmdbBytes + FromLmdbBytes + ?Sized,
    L: Layout + LayoutDupsort,
{
    #[inline]
    fn seek_k_nearest_v<'access>(
        &mut self,
        access: &'access ConstAccessor<'t>,
        key: &K,
        val: &V,
    ) -> Result<&'access V> {
        self.0.seek_k_nearest_v(&access.as_lmdb(), key, val)
    }
}

pub trait CursorAsXFrom<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: FromLmdbBytes + ?Sized,
    L: Layout,
{
    fn seek_k<'access>(
        &mut self,
        access: &'access ConstAccessor<'t>,
        key: &K,
    ) -> Result<&'access V>;
}

impl<'t, 'd, K, V, L> CursorAsXFrom<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: FromLmdbBytes + ?Sized,
    L: Layout,
{
    #[inline]
    fn seek_k<'access>(
        &mut self,
        access: &'access ConstAccessor<'t>,
        key: &K,
    ) -> Result<&'access V> {
        self.0.seek_k(&access.as_lmdb(), key)
    }
}

pub trait CursorAsFromXFrom<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + FromLmdbBytes + ?Sized,
    V: FromLmdbBytes + ?Sized,
    L: Layout,
{
    t_seek_both!(seek_k_both);
    t_seek_both!(seek_range_k);
}

impl<'t, 'd, K, V, L> CursorAsFromXFrom<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + FromLmdbBytes + ?Sized,
    V: FromLmdbBytes + ?Sized,
    L: Layout,
{
    c_seek_both!(seek_k_both);
    c_seek_both!(seek_range_k);
}

pub trait CursorAsXFromReservedSizedNonDupsort<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: FromReservedLmdbBytes + Sized,
    L: Layout + LayoutNoDuplicates,
{
    t_change_in_place!(reserve);
    t_change_in_place!(overwrite_in_place);
}

impl<'t, 'd, K, V, L> CursorAsXFromReservedSizedNonDupsort<'t, 'd, K, V, L>
    for Cursor<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: FromReservedLmdbBytes + Sized,
    L: Layout + LayoutNoDuplicates,
{
    c_change_in_place!(reserve);
    c_change_in_place!(overwrite_in_place);
}

pub trait CursorAsXRawDupfixed<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: LmdbRaw,
    L: Layout + LayoutDupsort + LayoutDupfixed,
{
    t_put!(put_multiple, [V], usize);
}

impl<'t, 'd, K, V, L> CursorAsXRawDupfixed<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: LmdbRaw,
    L: Layout + LayoutDupsort + LayoutDupfixed,
{
    c_put!(put_multiple, [V], usize);
}

pub trait CursorAsXFromReservedNonDupsort<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: FromReservedLmdbBytes + ?Sized,
    L: Layout + LayoutNoDuplicates,
{
    t_change_in_place_unsized!(reserve_unsized);
    t_change_in_place_unsized!(overwrite_in_place_unsized);
}

impl<'t, 'd, K, V, L> CursorAsXFromReservedNonDupsort<'t, 'd, K, V, L> for Cursor<'t, 'd, K, V, L>
where
    K: AsLmdbBytes + ?Sized,
    V: FromReservedLmdbBytes + ?Sized,
    L: Layout + LayoutNoDuplicates,
{
    c_change_in_place_unsized!(reserve_unsized);
    c_change_in_place_unsized!(overwrite_in_place_unsized);
}
