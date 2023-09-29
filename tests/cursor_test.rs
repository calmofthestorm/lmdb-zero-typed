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

macro_rules! pr {
    ($a:expr) => {
        $a.as_slice()
    };
    ($a:expr, $b:expr) => {
        (pr!($a), pr!($b))
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

    let opts3 = lmdb_zero::DatabaseOptions::new(
        lmdb_zero::db::INTEGERDUP
            | lmdb_zero::db::INTEGERKEY
            | lmdb_zero::db::CREATE
            | lmdb_zero::db::DUPFIXED
            | lmdb_zero::db::DUPSORT,
    );
    let db_dupfixed =
        Database::<Raw, Raw, LmdbLayoutDupfixed>::open(&env, Some("tree3"), &opts3).unwrap();

    let txn = WriteTransaction::new(&env).unwrap();
    let mut access = txn.access();

    let put_flags = lmdb_zero::put::Flags::empty();

    let mut c_u = txn.cursor(&db_unique).unwrap();
    let mut c_ds = txn.cursor(&db_dupsort).unwrap();
    let mut c_df = txn.cursor(&db_dupfixed).unwrap();

    common_ops(&mut access, &mut c_u, 0);
    common_ops(&mut access, &mut c_ds, 0);
    common_ops(&mut access, &mut c_df, 0);

    // Write (2, 10), (4, 20), and (6, 30) different ways to all three, then
    // verify. For dupsort, also write (4, 24), (4, 25), and (4, 26).
    c_u.reserve(&mut access, r!(2), put_flags).unwrap().0 = 10;

    c_u.reserve(&mut access, r!(4), put_flags).unwrap().0 = 21;
    c_u.overwrite_in_place(&mut access, r!(4), put_flags)
        .unwrap()
        .0 = 20;

    unsafe { c_u.reserve_unsized(&mut access, r!(6), std::mem::size_of::<Raw>(), put_flags) }
        .unwrap()
        .0 = 32;
    unsafe {
        c_u.overwrite_in_place_unsized(&mut access, r!(6), std::mem::size_of::<Raw>(), put_flags)
    }
    .unwrap()
    .0 = 30;

    c_ds.put(&mut access, r!(2), r!(10), put_flags).unwrap();

    c_ds.put(&mut access, r!(4), r!(24), put_flags).unwrap();
    c_ds.put(&mut access, r!(4), r!(25), put_flags).unwrap();
    c_ds.put(&mut access, r!(4), r!(26), put_flags).unwrap();
    c_ds.overwrite(&mut access, r!(4), r!(25), put_flags)
        .unwrap();

    c_ds.put(&mut access, r!(6), r!(30), put_flags).unwrap();
    c_ds.put(&mut access, r!(4), r!(20), put_flags).unwrap();

    c_df.put(&mut access, r!(2), r!(10), put_flags).unwrap();
    c_df.put(&mut access, r!(6), r!(30), put_flags).unwrap();
    assert_eq!(
        c_df.put_multiple(&mut access, r!(4), &[Raw(24), Raw(20), Raw(25)], put_flags)
            .unwrap(),
        3
    );
    c_df.put(&mut access, r!(4), r!(26), put_flags).unwrap();

    // Verify the written data using common operations.
    common_ops(&mut access, &mut c_u, 1);
    common_ops(&mut access, &mut c_ds, 1);
    common_ops(&mut access, &mut c_df, 1);

    fn common_ops<'t, 'd, 'a, L>(
        a: &'a mut WriteAccessor<'t>,
        c: &mut Cursor<'t, 'd, Raw, Raw, L>,
        s: u8,
    ) where
        L: Layout,
    {
        let put_flags = lmdb_zero::put::Flags::empty();
        let del_flags = lmdb_zero::del::NODUPDATA;

        if s == 0 {
            c.put(a, r!(8), r!(39), put_flags).unwrap();
            c.overwrite(a, r!(8), r!(40), put_flags).unwrap();
        } else {
            // The second two cursors have duplicates on key 4. The
            // operations here have been carefully chosen to be the same for
            // all three cursors even though they have different values.
            assert_eq!(c.first(a).unwrap(), (r!(2, 10)));
            assert_eq!(c.get_current(a).unwrap(), (r!(2, 10)));
            assert_eq!(c.last(a).unwrap(), (r!(8, 40)));
            assert_eq!(c.prev(a).unwrap(), (r!(6, 30)));
            assert_eq!(c.get_current(a).unwrap(), (r!(6, 30)));
            assert_eq!(c.prev_nodup(a).unwrap().0, r!(4));
            assert_eq!(c.prev_nodup(a).unwrap(), (r!(2, 10)));
            assert_eq!(c.next(a).unwrap(), (r!(4, 20)));
            assert_eq!(c.prev(a).unwrap(), (r!(2, 10)));
            assert!(c.prev(a).to_opt().unwrap().is_none());
            assert_eq!(c.seek_k_both(a, r!(6)).unwrap(), (r!(6, 30)));
            assert_eq!(c.next(a).unwrap(), (r!(8, 40)));
            assert!(c.next(a).to_opt().unwrap().is_none());
            assert!(c.next_nodup(a).to_opt().unwrap().is_none());
            assert_eq!(c.seek_range_k(a, r!(3)).unwrap(), (r!(4, 20)));
            assert_eq!(c.next_nodup(a).unwrap(), (r!(6, 30)));
            assert_eq!(c.next(a).unwrap(), (r!(8, 40)));
            assert!(c.seek_k(a, r!(3)).to_opt().unwrap().is_none());
            assert_eq!(c.seek_k(a, r!(6)).unwrap(), (r!(30)));

            c.del(a, del_flags).unwrap();
            assert_eq!(c.seek_k(a, r!(4)).unwrap(), (r!(20)));
            c.del(a, del_flags).unwrap();
            assert_eq!(c.seek_k(a, r!(8)).unwrap(), (r!(40)));
            c.del(a, del_flags).unwrap();
            assert_eq!(c.seek_k(a, r!(2)).unwrap(), (r!(10)));
            c.del(a, del_flags).unwrap();

            assert!(c.first(a).to_opt().unwrap().is_none());
        }
    }

    dupsort_ops(&mut access, &mut c_ds);
    dupsort_ops(&mut access, &mut c_df);

    fn dupsort_ops<'t, 'd, 'a, L>(a: &'a mut WriteAccessor<'t>, c: &mut Cursor<'t, 'd, Raw, Raw, L>)
    where
        L: Layout + LayoutDupsort,
    {
        let put_flags = lmdb_zero::put::Flags::empty();
        let del_flags = lmdb_zero::del::Flags::empty();

        for j in 0..200 {
            c.put(a, r!(1), r!(j * 2), put_flags).unwrap();
            c.put(a, r!(3), r!(j * 2 + 10000), put_flags).unwrap();
        }

        assert_eq!(c.first(a).unwrap(), (r!(1, 0)));
        for j in 1..200 {
            assert_eq!(c.next(a).unwrap(), (r!(1, j * 2)));
        }

        for j in 0..200 {
            assert_eq!(c.next(a).unwrap(), (r!(3, j * 2 + 10000)));
        }
        assert!(c.next(a).to_opt().unwrap().is_none());

        assert!(c.seek_kv(&r!(3), &r!(13099)).to_opt().unwrap().is_none());
        c.seek_kv(&r!(3), &r!(10198)).unwrap();

        for j in 100..200 {
            assert_eq!(c.next(a).unwrap(), (r!(3, j * 2 + 10000)));
        }
        assert!(c.next_nodup(a).to_opt().unwrap().is_none());

        assert_eq!(c.prev_nodup(a).unwrap(), (r!(1, 398)));
        assert!(c.prev_nodup(a).to_opt().unwrap().is_none());
        assert!(c.next_dup(a).to_opt().unwrap().is_none());
        assert_eq!(c.next(a).unwrap(), (r!(3, 10000)));
        assert_eq!(c.next_dup(a).unwrap(), (r!(3, 10002)));
        assert_eq!(c.prev_dup(a).unwrap(), (r!(3, 10000)));
        assert!(c.prev_dup(a).to_opt().unwrap().is_none());
        assert_eq!(c.prev(a).unwrap(), (r!(1, 398)));
        assert_eq!(c.first_dup(a).unwrap(), (r!(0)));
        assert_eq!(c.last_dup(a).unwrap(), (r!(398)));
        assert_eq!(c.prev(a).unwrap(), (r!(1, 396)));
        assert_eq!(c.next_nodup(a).unwrap(), (r!(3, 10000)));
        assert_eq!(c.next(a).unwrap(), (r!(3, 10002)));
        assert_eq!(c.first_dup(a).unwrap(), (r!(10000)));
        assert_eq!(c.last_dup(a).unwrap(), (r!(10398)));

        assert_eq!(
            c.seek_k_nearest_v(a, &r!(3), &r!(10199)).unwrap(),
            r!(10200)
        );
        assert_eq!(c.next(a).unwrap(), (r!(3, 10202)));
        assert_eq!(c.count().unwrap(), 200);
        c.del(a, del_flags).unwrap();
        c.seek_k_nearest_v(a, &r!(3), &r!(10199)).unwrap();
        assert_eq!(c.count().unwrap(), 199);
    }

    for j in 0..86400 {
        c_df.put(&mut access, r!(7), r!(j), put_flags).unwrap();
    }

    assert_eq!(c_df.first_dup(&access).unwrap(), (r!(0)));
    let mut replay = 0;
    for entry in c_df.get_multiple(&access).unwrap() {
        assert_eq!(replay, { entry.0 });
        replay += 1;
    }
    while replay < 86400 {
        for entry in c_df.next_multiple(&access).unwrap() {
            assert_eq!(replay, { entry.0 });
            replay += 1;
        }
    }
    assert_eq!(replay, 86400);
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
    let _db4 = Database::<Raw, Raw, LmdbLayoutDefault>::open(&env, Some("tree4"), &opts).unwrap();
    let db5 = Database::<[u8], [u8], LmdbLayoutDefault>::open(&env, Some("tree5"), &opts).unwrap();

    let txn = WriteTransaction::new(&env).unwrap();
    let mut access = txn.access();

    let put_flags = lmdb_zero::put::Flags::empty();
    let del_flags = lmdb_zero::del::Flags::empty();

    access.put(&db1, b"h", b"v ", put_flags).unwrap();
    access.put(&db1, b"goo", b"", put_flags).unwrap();

    // We mostly care that these compile; the tests don't need to be that
    // interesting.

    let mut c1 = txn.cursor(&db1).unwrap();
    assert_eq!(c1.last(&access).unwrap(), pr!(b"h", b"v "));
    assert_eq!(c1.get_current(&access).unwrap(), pr!(b"h", b"v "));
    assert_eq!(c1.prev(&access).unwrap(), pr!(b"goo", b""));
    assert_eq!(c1.next_nodup(&access).unwrap(), pr!(b"h", b"v "));
    assert_eq!(c1.prev_nodup(&access).unwrap(), pr!(b"goo", b""));
    assert_eq!(c1.first(&access).unwrap(), pr!(b"goo", b""));
    assert_eq!(c1.next(&access).unwrap(), pr!(b"h", b"v "));
    assert_eq!(c1.seek_k(&access, b"h").unwrap(), pr!(b"v "));
    assert!(c1.seek_k(&access, b"x").to_opt().unwrap().is_none());
    assert_eq!(c1.seek_k_both(&access, b"h").unwrap(), pr!(b"h", b"v "));
    assert!(c1.seek_k_both(&access, b"x").to_opt().unwrap().is_none());
    assert_eq!(c1.seek_range_k(&access, b"a").unwrap(), pr!(b"goo", b""));
    assert_eq!(c1.seek_range_k(&access, b"good").unwrap(), pr!(b"h", b"v "));
    assert!(c1.seek_range_k(&access, b"z").to_opt().unwrap().is_none());

    assert_eq!(c1.first(&access).unwrap(), pr!(b"goo", b""));
    c1.del(&mut access, del_flags).unwrap();
    assert_eq!(c1.last(&access).unwrap(), pr!(b"h", b"v "));
    c1.overwrite(&mut access, b"h", b"da", put_flags).unwrap();
    assert_eq!(c1.last(&access).unwrap(), pr!(b"h", b"da"));
    unsafe { c1.overwrite_in_place_unsized(&mut access, b"h", 2, put_flags) }
        .unwrap()
        .copy_from_slice(b"kz");
    assert_eq!(c1.first(&access).unwrap(), pr!(b"h", b"kz"));
    unsafe { c1.reserve_unsized(&mut access, b"zailor", 3, put_flags) }
        .unwrap()
        .copy_from_slice(b"abc");
    assert_eq!(c1.first(&access).unwrap(), pr!(b"h", b"kz"));
    assert_eq!(c1.last(&access).unwrap(), pr!(b"zailor", b"abc"));

    let mut c5 = txn.cursor(&db5).unwrap();
    assert!(c5.last(&access).to_opt().unwrap().is_none());
    assert!(c5.first(&access).to_opt().unwrap().is_none());
    assert!(c5.next(&access).to_opt().unwrap().is_none());
    assert!(c5.next_nodup(&access).to_opt().unwrap().is_none());
    assert!(c5.get_current(&access).is_err());
    assert!(c5.prev(&access).to_opt().unwrap().is_none());
    assert!(c5.prev_nodup(&access).to_opt().unwrap().is_none());
    assert!(c5.seek_k(&access, b"j").to_opt().unwrap().is_none());
    assert!(c5.seek_k_both(&access, b"j").to_opt().unwrap().is_none());
    assert!(c5.seek_range_k(&access, b"z").to_opt().unwrap().is_none());

    let mut c2 = txn.cursor(&db2).unwrap();
    c2.put(&mut access, &Raw(5), b"key", put_flags).unwrap();
    assert_eq!(c2.get_current(&access).unwrap(), (&Raw(5), pr!(b"key")));
    c2.put(&mut access, &Raw(7), b"hello", put_flags).unwrap();
    assert_eq!(c2.get_current(&access).unwrap(), (&Raw(7), pr!(b"hello")));
    unsafe { c2.reserve_unsized(&mut access, &Raw(343), 3, put_flags) }
        .unwrap()
        .copy_from_slice(b"abc");
    assert_eq!(c2.get_current(&access).unwrap(), (&Raw(343), pr!(b"abc")));
    unsafe { c2.overwrite_in_place_unsized(&mut access, &Raw(343), 3, put_flags) }
        .unwrap()
        .copy_from_slice(b"def");
    assert_eq!(c2.get_current(&access).unwrap(), (&Raw(343), pr!(b"def")));
    assert_eq!(
        c2.seek_k_both(&access, &Raw(7)).unwrap(),
        (&Raw(7), pr!(b"hello"))
    );
    c2.overwrite(&mut access, &Raw(7), b"byeby", put_flags)
        .unwrap();
    assert_eq!(c2.get_current(&access).unwrap(), (&Raw(7), pr!(b"byeby")));

    assert_eq!(c2.last(&access).unwrap(), (&Raw(343), pr!(b"def")));
    assert_eq!(c2.get_current(&access).unwrap(), (&Raw(343), pr!(b"def")));
    assert_eq!(c2.prev(&access).unwrap(), (&Raw(7), pr!(b"byeby")));
    assert_eq!(c2.next_nodup(&access).unwrap(), (&Raw(343), pr!(b"def")));
    assert_eq!(c2.prev_nodup(&access).unwrap(), (&Raw(7), pr!(b"byeby")));
    assert_eq!(c2.prev(&access).unwrap(), (&Raw(5), pr!(b"key")));
    assert_eq!(c2.next(&access).unwrap(), (&Raw(7), pr!(b"byeby")));
    assert_eq!(c2.first(&access).unwrap(), (&Raw(5), pr!(b"key")));
    assert_eq!(c2.seek_k(&access, &Raw(7)).unwrap(), pr!(b"byeby"));
    assert_eq!(
        c2.seek_k_both(&access, &Raw(5)).unwrap(),
        (&Raw(5), pr!(b"key"))
    );
    assert_eq!(
        c2.seek_range_k(&access, &Raw(300)).unwrap(),
        (&Raw(343), pr!(b"def"))
    );
    assert!(c2
        .seek_range_k(&access, &Raw(400))
        .to_opt()
        .unwrap()
        .is_none());

    assert_eq!(c2.last(&access).unwrap(), (&Raw(343), pr!(b"def")));
    c2.del(&mut access, del_flags).unwrap();
    assert_eq!(c2.last(&access).unwrap(), (&Raw(7), pr!(b"byeby")));
    c2.del(&mut access, del_flags).unwrap();
    assert_eq!(c2.last(&access).unwrap(), (&Raw(5), pr!(b"key")));
    c2.del(&mut access, del_flags).unwrap();
    assert!(c2.first(&access).to_opt().unwrap().is_none());

    let mut c3 = txn.cursor(&db3).unwrap();
    c3.put(&mut access, b"key", &Raw(5), put_flags).unwrap();
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"key"), &Raw(5)));
    c3.put(&mut access, b"hello", &Raw(7), put_flags).unwrap();
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"hello"), &Raw(7)));
    *unsafe { c3.reserve_unsized(&mut access, b"hx", std::mem::size_of::<Raw>(), put_flags) }
        .unwrap() = Raw(61);
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"hx"), &Raw(61)));
    *c3.reserve(&mut access, b"hxe", put_flags).unwrap() = Raw(6119);
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"hxe"), &Raw(6119)));
    *unsafe {
        c3.overwrite_in_place_unsized(&mut access, b"hxe", std::mem::size_of::<Raw>(), put_flags)
    }
    .unwrap() = Raw(11);
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"hxe"), &Raw(11)));
    *c3.overwrite_in_place(&mut access, b"hxe", put_flags)
        .unwrap() = Raw(1);
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"hxe"), &Raw(1)));
    assert_eq!(
        c3.seek_k_both(&access, b"hello").unwrap(),
        (pr!(b"hello"), &Raw(7))
    );
    c3.overwrite(&mut access, b"hello", &Raw(711), put_flags)
        .unwrap();
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"hello"), &Raw(711)));
    *c3.reserve(&mut access, b"hello2", put_flags).unwrap() = Raw(83);
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"hello2"), &Raw(83)));

    assert_eq!(c3.last(&access).unwrap(), (pr!(b"key"), &Raw(5)));
    assert_eq!(c3.get_current(&access).unwrap(), (pr!(b"key"), &Raw(5)));
    assert_eq!(c3.prev(&access).unwrap(), (pr!(b"hxe"), &Raw(1)));
    assert_eq!(c3.next_nodup(&access).unwrap(), (pr!(b"key"), &Raw(5)));
    assert_eq!(c3.prev_nodup(&access).unwrap(), (pr!(b"hxe"), &Raw(1)));
    assert_eq!(c3.prev(&access).unwrap(), (pr!(b"hx"), &Raw(61)));
    assert_eq!(c3.next(&access).unwrap(), (pr!(b"hxe"), &Raw(1)));
    assert_eq!(c3.first(&access).unwrap(), (pr!(b"hello"), &Raw(711)));
    assert_eq!(c3.seek_k(&access, b"key").unwrap(), &Raw(5));
    assert_eq!(
        c3.seek_k_both(&access, b"key").unwrap(),
        (pr!(b"key"), &Raw(5))
    );
    assert_eq!(
        c3.seek_range_k(&access, b"key").unwrap(),
        (pr!(b"key"), &Raw(5))
    );
    assert!(c3.seek_range_k(&access, b"ze").to_opt().unwrap().is_none());

    assert_eq!(c3.last(&access).unwrap(), (pr!(b"key"), &Raw(5)));
    c3.del(&mut access, del_flags).unwrap();
    assert_eq!(c3.last(&access).unwrap(), (pr!(b"hxe"), &Raw(1)));
    c3.del(&mut access, del_flags).unwrap();
    assert_eq!(c3.last(&access).unwrap(), (pr!(b"hx"), &Raw(61)));
    c3.del(&mut access, del_flags).unwrap();
    assert_eq!(c3.last(&access).unwrap(), (pr!(b"hello2"), &Raw(83)));
    c3.del(&mut access, del_flags).unwrap();
    assert_eq!(c3.first(&access).unwrap(), (pr!(b"hello"), &Raw(711)));
    c3.del(&mut access, del_flags).unwrap();
    assert!(c3.last(&access).to_opt().unwrap().is_none());
}
