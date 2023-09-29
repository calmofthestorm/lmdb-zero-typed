pub mod accessor;
pub mod cursor;
pub mod cursor_iter;
pub mod database;
pub mod layout;
pub mod traits;
pub mod transaction;

pub use crate::cursor::*;
pub use accessor::*;
pub use cursor_iter::*;
pub use database::*;
pub use layout::*;
pub use transaction::*;

#[macro_export]
macro_rules! cursor {
    ($name:ident, $cursor:ident, $db:ident, $desc:expr) => {
        pub fn $name<'txn, 'db>(
            &'db self,
            txn: &'txn ConstTransaction<'e>,
        ) -> Result<$cursor<'txn, 'db>>
        where
            'e: 'db,
        {
            txn.cursor(&self.$db).context($desc)
        }
    };
}

#[macro_export]
macro_rules! types {
    ($cursor:ident, $db:ident, $key:ty, $value:ty, $layout:ty) => {
        pub type $cursor<'t, 'd> = Cursor<'t, 'd, $key, $value, $layout>;
        pub type $db<'e> = Database<'e, $key, $value, $layout>;
    };
}

#[macro_export]
macro_rules! db {
    ($env:expr, $desc: expr, $options:expr) => {
        Arc::new(Database::open(
            $env.clone(),
            Some($desc),
            &DatabaseOptions::new(db::CREATE | $options),
        )?)
    };
}
