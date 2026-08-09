use sqlx::sqlite::SqlitePool;

pub struct Database {
    pub(super) pool: SqlitePool,
}
