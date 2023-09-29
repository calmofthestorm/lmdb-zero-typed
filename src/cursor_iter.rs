use lmdb_zero::traits::LmdbResultExt;
use lmdb_zero::{MaybeOwned, Result};

use crate::{ConstAccessor, Cursor, Layout};

pub struct CursorIter<'a, 'access: 'a, 'txn: 'access, 'db: 'txn, K, V, T, L>
where
    L: Layout,
    K: ?Sized,
    V: ?Sized,
{
    cursor: MaybeOwned<'a, Cursor<'txn, 'db, K, V, L>>,
    access: &'access ConstAccessor<'txn>,
    head: Option<T>,
    next: fn(&mut Cursor<'txn, 'db, K, V, L>, &'access ConstAccessor<'txn>) -> Result<T>,
}

impl<'a, 'access: 'a, 'txn: 'access, 'db: 'txn, K, V, T, L>
    CursorIter<'a, 'access, 'txn, 'db, K, V, T, L>
where
    L: Layout,
    K: ?Sized,
    V: ?Sized,
{
    #[inline]
    pub fn new<
        H: FnOnce(&mut Cursor<'txn, 'db, K, V, L>, &'access ConstAccessor<'txn>) -> Result<T>,
    >(
        mut cursor: MaybeOwned<'a, Cursor<'txn, 'db, K, V, L>>,
        access: &'access ConstAccessor<'txn>,
        head: H,
        next: fn(&mut Cursor<'txn, 'db, K, V, L>, &'access ConstAccessor<'txn>) -> Result<T>,
    ) -> Result<Self> {
        let head_val = head(&mut *cursor, access).to_opt()?;
        Ok(CursorIter {
            cursor: cursor,
            access: access,
            head: head_val,
            next: next,
        })
    }
}

impl<'a, 'access: 'a, 'txn: 'access, 'db: 'txn, K, V, T, L: Layout> Iterator
    for CursorIter<'a, 'access, 'txn, 'db, K, V, T, L>
where
    K: ?Sized,
    V: ?Sized,
{
    type Item = Result<T>;

    #[inline]
    fn next(&mut self) -> Option<Result<T>> {
        if let Some(head) = self.head.take() {
            Some(Ok(head))
        } else {
            match (self.next)(&mut *self.cursor, self.access).to_opt() {
                Ok(Some(v)) => Some(Ok(v)),
                Ok(None) => None,
                Err(err) => Some(Err(err.into())),
            }
        }
    }
}
