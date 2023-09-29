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
fn test_dup() {
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
    let db_unique =
        Database::<Raw, Raw, LmdbLayoutDefault>::open(&env, Some("tree1"), &opts1).unwrap();

    let opts2 = lmdb_zero::DatabaseOptions::new(
        lmdb_zero::db::INTEGERDUP
            | lmdb_zero::db::INTEGERKEY
            | lmdb_zero::db::CREATE
            | lmdb_zero::db::DUPSORT,
    );
    let db_dupsort =
        Database::<Raw, Raw, LmdbLayoutDupsort>::open(&env, Some("tree2"), &opts2).unwrap();

    let txn = WriteTransaction::new(&env).unwrap();
    let mut access = txn.access();

    let put_flags = lmdb_zero::put::Flags::empty();

    access.put(&db_unique, r!(3), r!(7), put_flags).unwrap();
    access.put(&db_unique, r!(3), r!(3), put_flags).unwrap();
    access.put(&db_unique, r!(3), r!(7), put_flags).unwrap();
    access.put(&db_unique, r!(3), r!(11), put_flags).unwrap();
    access.put_reserve(&db_unique, r!(4), put_flags).unwrap().0 = 14;
    access.put_reserve(&db_unique, r!(0), put_flags).unwrap().0 = 15;

    access.put_reserve(&db_dupsort, r!(3), put_flags).unwrap().0 = 13;
    access.put(&db_dupsort, r!(3), r!(11), put_flags).unwrap();
    access.put(&db_dupsort, r!(3), r!(14), put_flags).unwrap();
    access.put(&db_dupsort, r!(3), r!(13), put_flags).unwrap();

    unsafe {
        access.put_reserve_unsized(&db_unique, r!(10), std::mem::size_of::<Raw>(), put_flags)
    }
    .unwrap()
    .0 = 100;
    unsafe {
        access.put_reserve_unsized(&db_dupsort, r!(10), std::mem::size_of::<Raw>(), put_flags)
    }
    .unwrap()
    .0 = 100;

    assert_eq!(access.get(&db_unique, r!(3)).unwrap(), r!(11));
    assert_eq!(access.get(&db_unique, r!(4)).unwrap(), r!(14));
    assert_eq!(access.get(&db_unique, r!(0)).unwrap(), r!(15));
    assert_eq!(access.get(&db_unique, r!(10)).unwrap(), r!(100));

    assert_eq!(access.get(&db_dupsort, r!(3)).unwrap(), r!(11));
    assert_eq!(access.get(&db_dupsort, r!(10)).unwrap(), r!(100));

    let mut c = txn.cursor(&db_dupsort).unwrap();
    assert_eq!(c.first(&mut access).unwrap(), r!(3, 11));
    assert_eq!(c.next(&mut access).unwrap(), r!(3, 13));
    assert_eq!(c.next(&mut access).unwrap(), r!(3, 14));
    assert_eq!(c.next(&mut access).unwrap(), r!(10, 100));
    assert!(c.next(&mut access).to_opt().unwrap().is_none());

    // Missing key and value.
    assert!(access
        .del_item(&db_dupsort, r!(2), r!(2))
        .to_opt()
        .unwrap()
        .is_none());

    // Deleted.
    assert_eq!(access.del_item(&db_dupsort, r!(3), r!(13)).unwrap(), ());

    assert_eq!(c.first(&mut access).unwrap(), r!(3, 11));
    assert_eq!(c.next(&mut access).unwrap(), r!(3, 14));
    assert_eq!(c.next(&mut access).unwrap(), r!(10, 100));
    assert!(c.next(&mut access).to_opt().unwrap().is_none());

    assert!(access
        .del_key(&db_dupsort, r!(377))
        .to_opt()
        .unwrap()
        .is_none());
    assert_eq!(access.del_key(&db_dupsort, r!(3)).unwrap(), ());

    assert_eq!(access.get(&db_dupsort, r!(10)).unwrap(), r!(100));
    access.clear_db(&db_dupsort).unwrap();
    assert!(access.get(&db_dupsort, r!(10)).to_opt().unwrap().is_none());

    assert!(access
        .del_item(&db_unique, r!(377), r!(13))
        .to_opt()
        .unwrap()
        .is_none());
    assert_eq!(access.del_item(&db_unique, r!(4), r!(13)).unwrap(), ());

    assert_eq!(access.get(&db_unique, r!(3)).unwrap(), r!(11));
    assert_eq!(access.get(&db_unique, r!(0)).unwrap(), r!(15));
    assert_eq!(access.get(&db_unique, r!(10)).unwrap(), r!(100));
}

#[test]
fn test_repr_from_as_raw() {
    let tmp = tempdir::TempDir::new("unit.test").unwrap();

    let env = unsafe {
        let mut eb = lmdb_zero::EnvBuilder::new().unwrap();
        eb.set_mapsize(1_000_000).unwrap();
        eb.set_maxdbs(5).unwrap();

        eb.set_maxreaders(64).unwrap();

        eb.open(&tmp.path().to_string_lossy(), lmdb_zero::open::NOTLS, 0o600)
            .unwrap()
    };

    let opts = lmdb_zero::DatabaseOptions::new(lmdb_zero::db::CREATE);

    let db1 = Database::<[u8], [u8], LmdbLayoutDefault>::open(&env, Some("tree1"), &opts).unwrap();
    let db2 = Database::<Raw, [u8], LmdbLayoutDefault>::open(&env, Some("tree2"), &opts).unwrap();
    let db3 = Database::<[u8], Raw, LmdbLayoutDefault>::open(&env, Some("tree3"), &opts).unwrap();
    let db4 = Database::<Raw, Raw, LmdbLayoutDefault>::open(&env, Some("tree4"), &opts).unwrap();

    let txn = WriteTransaction::new(&env).unwrap();
    let mut access = txn.access();

    let put_flags = lmdb_zero::put::Flags::empty();

    access.put(&db1, b"k1", b"v1", put_flags).unwrap();
    access.put(&db2, r!(1), b"v1", put_flags).unwrap();
    access.put(&db3, b"k1", r!(1), put_flags).unwrap();
    access.put(&db4, r!(1), r!(1), put_flags).unwrap();

    access.put(&db1, b"k2", b"v2", put_flags).unwrap();
    access.put(&db2, r!(2), b"v2", put_flags).unwrap();
    access.put_reserve(&db3, b"k2", put_flags).unwrap().0 = 2;
    access.put_reserve(&db4, r!(2), put_flags).unwrap().0 = 2;

    access.put(&db1, b"k3", b"v3", put_flags).unwrap();
    access.put(&db2, r!(3), b"v3", put_flags).unwrap();
    unsafe { access.put_reserve_unsized(&db3, b"k3", std::mem::size_of::<Raw>(), put_flags) }
        .unwrap()
        .0 = 3;
    unsafe { access.put_reserve_unsized(&db4, r!(3), std::mem::size_of::<Raw>(), put_flags) }
        .unwrap()
        .0 = 3;

    assert_eq!(access.get(&db1, b"k1").unwrap(), (b"v1"));
    assert_eq!(access.get(&db1, b"k2").unwrap(), (b"v2"));
    assert_eq!(access.get(&db1, b"k3").unwrap(), (b"v3"));

    assert_eq!(access.get(&db2, r!(1)).unwrap(), (b"v1"));
    assert_eq!(access.get(&db2, r!(2)).unwrap(), (b"v2"));
    assert_eq!(access.get(&db2, r!(3)).unwrap(), (b"v3"));

    assert_eq!(access.get(&db3, b"k1").unwrap(), r!(1));
    assert_eq!(access.get(&db3, b"k2").unwrap(), r!(2));
    assert_eq!(access.get(&db3, b"k3").unwrap(), r!(3));

    assert_eq!(access.get(&db4, r!(1)).unwrap(), r!(1));
    assert_eq!(access.get(&db4, r!(2)).unwrap(), r!(2));
    assert_eq!(access.get(&db4, r!(3)).unwrap(), r!(3));
}
