// Copyright 2022 Singularity Data
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// use risingwave_common::error::{Result, RwError};

pub trait Sink {
    fn write_batch(&mut self);
}

// Primitive design of MySQLSink
#[allow(dead_code)]
pub struct MySQLSink {
    pub endpoint: String,
    pub table: String,
    pub database: String,
    pub user: String,
    pub password: String,
}

impl Sink for MySQLSink {
    fn write_batch(&mut self) {
        todo!();
    }
}

pub struct RedisSink;

impl Sink for RedisSink {
    fn write_batch(&mut self) {
        todo!();
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
struct Payment {
    customer_id: i32,
    amount: i32,
    account_name: Option<String>,
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_basic() -> Result<(), mysql::Error> {
        use mysql::*;
        use mysql::prelude::*;
        
        #[derive(Debug, PartialEq, Eq)]
        struct Payment {
            customer_id: i32,
            amount: i32,
            account_name: Option<String>,
        }
        
        let opts = Opts::from_url("mysql://nander:123@localhost:3306/db1")?;
        
        let pool = Pool::new(opts)?;
        
        let mut conn = pool.get_conn()?;
        
        // Let's create a table for payments.
        conn.query_drop(
            r"CREATE TEMPORARY TABLE payment (
                customer_id int not null,
                amount int not null,
                account_name text
            )")?;
        
        let payments = vec![
            Payment { customer_id: 1, amount: 2, account_name: None },
            Payment { customer_id: 3, amount: 4, account_name: Some("foo".into()) },
            Payment { customer_id: 5, amount: 6, account_name: None },
            Payment { customer_id: 7, amount: 8, account_name: None },
            Payment { customer_id: 9, amount: 10, account_name: Some("bar".into()) },
        ];
        
        // Now let's insert payments to the database
        conn.exec_batch(
            r"INSERT INTO payment (customer_id, amount, account_name)
              VALUES (:customer_id, :amount, :account_name)",
            payments.iter().map(|p| params! {
                "customer_id" => p.customer_id,
                "amount" => p.amount,
                "account_name" => &p.account_name,
            })
        )?;
        
        // Let's select payments from database. Type inference should do the trick here.
        let selected_payments = conn
            .query_map(
                "SELECT customer_id, amount, account_name from payment",
                |(customer_id, amount, account_name)| {
                    Payment { customer_id, amount, account_name }
                },
            )?;
        
        // Let's make sure, that `payments` equals to `selected_payments`.
        // Mysql gives no guaranties on order of returned rows
        // without `ORDER BY`, so assume we are lucky.
        assert_eq!(payments, selected_payments);

        Ok(())
    }
}