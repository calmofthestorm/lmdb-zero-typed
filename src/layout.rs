pub struct LmdbLayoutDefault;
pub struct LmdbLayoutDupsort;
pub struct LmdbLayoutDupfixed;

pub trait Layout {}
pub trait LayoutNoDuplicates {}
pub trait LayoutDupsort {}
pub trait LayoutDupfixed {}

impl LayoutDupsort for LmdbLayoutDupsort {}
impl LayoutDupsort for LmdbLayoutDupfixed {}
impl LayoutDupfixed for LmdbLayoutDupfixed {}
impl Layout for LmdbLayoutDupsort {}
impl Layout for LmdbLayoutDupfixed {}
impl Layout for LmdbLayoutDefault {}
impl LayoutNoDuplicates for LmdbLayoutDefault {}
