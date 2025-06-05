use std::time::Duration;

use mysql_async::prelude::Queryable;
use mysql_async::{Conn, OptsBuilder, Row};
use tokio::pin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 创建数据库连接选项
    let mut opts_builder = OptsBuilder::default()
        .user(Some("root"))
        .pass(Some("")) // 替换为你的密码
        .ip_or_hostname("127.0.0.1")
        .tcp_port(8306)
        .db_name(Some("risedev"));

    // 创建数据库连接
    let conn = Conn::new(opts_builder).await?;

    // 删除表
    conn.exec_drop("DROP TABLE IF EXISTS test_table", ())
        .await?;

    // 创建表
    conn.exec_drop(
        "CREATE TABLE test_table (id INT PRIMARY KEY, value VARCHAR(255))",
        (),
    )
    .await?;

    // 插入数据
    for i in 1..200 {
        conn.exec_drop(
            "INSERT INTO test_table VALUES (?, ?) ON DUPLICATE KEY UPDATE value=?",
            (i, format!("value_{}", i), format!("value_{}", i)),
        )
        .await?;
    }

    // 设置时区并处理超时
    let q1 = conn.exec_drop("SET time_zone = if(not sleep(2), \"+00:00\", \"\")", ());
    match tokio::time::timeout(Duration::from_secs(1), q1).await {
        Ok(result) => {
            println!("Operation completed: {:?}", result);
        }
        Err(_) => {
            println!("Operation timed out");
        }
    }

    // 查询数据
    let rs_stream = "SELECT * FROM test_table WHERE id > 100 LIMIT 10"
        .stream::<Row, _>(&mut conn)
        .await?;
    pin!(rs_stream);

    while let Some(rs) = rs_stream.next().await {
        let row = rs?;
        println!("get row {:?}", row);
    }

    Ok(())
}
