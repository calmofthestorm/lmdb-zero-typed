use lmdb_zero::traits::*;

use lmdb_zero_typed::*;

macro_rules! r {
    ($a:expr) => {
        &Raw($a)
    };
    ($a:expr, $b:expr) => {
        (r!($a), r!($b))
    };
}

#[repr(C, packed)]
#[derive(Default, Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd)]
struct Raw(u32);

unsafe impl LmdbRaw for Raw {}

#[test]
fn test_cursor_iter() {
    let tmp = tempdir::TempDir::new("unit.test").unwrap();

    let env = unsafe {
        let mut eb = lmdb_zero::EnvBuilder::new().unwrap();
        eb.set_mapsize(1_000_000).unwrap();
        eb.set_maxdbs(5).unwrap();

        eb.set_maxreaders(64).unwrap();

        eb.open(&tmp.path().to_string_lossy(), lmdb_zero::open::NOTLS, 0o600)
            .unwrap()
    };

    let opts1 = lmdb_zero::DatabaseOptions::new(
        lmdb_zero::db::INTEGERDUP | lmdb_zero::db::INTEGERKEY | lmdb_zero::db::CREATE,
    );
    let db = Database::<Raw, Raw, LmdbLayoutDefault>::open(&env, Some("tree1"), &opts1).unwrap();

    let put_flags = lmdb_zero::put::Flags::empty();

    let txn = WriteTransaction::new(&env).unwrap();
    let mut access = txn.access();
    access.put(&db, r!(1), r!(2), put_flags).unwrap();
    access.put(&db, r!(2), r!(4), put_flags).unwrap();
    access.put(&db, r!(3), r!(6), put_flags).unwrap();

    let mut cursor = txn.cursor(&db).unwrap();

    let mut iter = CursorIter::new(
        lmdb_zero::MaybeOwned::Borrowed(&mut cursor),
        &access,
        |c, a| c.first(a),
        Cursor::next,
    )
    .unwrap();

    assert_eq!(iter.next().unwrap().unwrap(), r!(1, 2));
    assert_eq!(iter.next().unwrap().unwrap(), r!(2, 4));
    assert_eq!(iter.next().unwrap().unwrap(), r!(3, 6));
    assert!(iter.next().is_none());
}
