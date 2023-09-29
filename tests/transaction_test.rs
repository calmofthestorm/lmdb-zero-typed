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
fn test_transaction() {
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

    let mut txn = WriteTransaction::new(&env).unwrap();
    {
        let mut access = txn.access();

        access.put(&db, r!(1), r!(3), put_flags).unwrap();
        access.put(&db, r!(2), r!(4), put_flags).unwrap();
    }

    {
        let mut txn = txn.child_tx().unwrap();
        {
            let mut access = txn.access();

            access.put(&db, r!(3), r!(19), put_flags).unwrap();
            access.put(&db, r!(5), r!(22), put_flags).unwrap();
        }

        {
            let txn = txn.child_tx().unwrap();
            {
                let mut access = txn.access();

                access.put(&db, r!(300), r!(1900), put_flags).unwrap();
                access.put(&db, r!(500), r!(2200), put_flags).unwrap();
            }
        }

        txn.commit().unwrap();
    }

    {
        let mut access = txn.access();
        access.put(&db, r!(7), r!(70), put_flags).unwrap();

        assert_eq!(access.get(&db, r!(1)).unwrap(), r!(3));
        assert_eq!(access.get(&db, r!(2)).unwrap(), r!(4));
        assert_eq!(access.get(&db, r!(3)).unwrap(), r!(19));
        assert_eq!(access.get(&db, r!(5)).unwrap(), r!(22));
        assert!(access.get(&db, r!(300)).to_opt().unwrap().is_none());
        assert!(access.get(&db, r!(500)).to_opt().unwrap().is_none());
        assert_eq!(access.get(&db, r!(7)).unwrap(), r!(70));
    }

    txn.commit().unwrap();

    let txn = ReadTransaction::new(&env).unwrap();

    let access = txn.access();
    assert_eq!(access.get(&db, r!(1)).unwrap(), r!(3));
    assert_eq!(access.get(&db, r!(2)).unwrap(), r!(4));
    assert_eq!(access.get(&db, r!(3)).unwrap(), r!(19));
    assert_eq!(access.get(&db, r!(5)).unwrap(), r!(22));
    assert!(access.get(&db, r!(300)).to_opt().unwrap().is_none());
    assert!(access.get(&db, r!(500)).to_opt().unwrap().is_none());
    assert_eq!(access.get(&db, r!(7)).unwrap(), r!(70));
}
