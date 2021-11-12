pub struct Transaction {

}

/// Like the snapshot in PostgreSQL, this struct serves to use `Snapshot Isolation`
/// concurrency control to set up the read view of the data achieving `Serializble`.
/// Design Note: It is not truely `Serializable` as `Write Skew` could happen when two transactions 
/// reads and writes in a crossed manner. For example, `T1` reads from `a` and writes to `b` 
/// and `T2` reads from `b` and writes to `a`. This could happen in our system when two source 
/// executors read and write values from different keyed CDC streams. The expected serializable 
/// result of `T1` and `T2`would either be both `a`s or `b`s. However both transactions will get 
/// a `SnapshotData` to fetch `a` and `b` independently, and the finaly result of two transactions 
/// will be either `(a, b)` or `(b, a)`.
pub struct SnapshotData {
    /// `xmin`: all transactions smaller than `xmin` are done and visible.
    xmin: u64,

    /// `xmax`: all transactions bigger than `u64` are active or unassigned.
    xmax: u64,

    /// `xip`: list of active transactions between `xmin` and `xmax` (not visible).
    xip: Vec<u64>, // aka. transactions in process
}

pub enum IsolationLevel {
    Serializable, // with `Snapshot Isolation`
}

/// A `TransactionManager` manages transactions of a database service.
/// In cloud deployment, it should support transactions within a provisioned database service.
pub trait TransactionManager: Sync + Send {
    /// Return the transaction id.
    fn GetTransactionId(&self) -> Result<u64>;

    /// Return the transaction snapshot.
    fn GetTransactionSnapshot(&self, trx_id: u64, isolation_level: IsolationLevel) -> Result();
}