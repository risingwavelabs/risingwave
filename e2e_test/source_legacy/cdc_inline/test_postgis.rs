#!/usr/bin/env -S cargo -Zscript
---cargo
[dependencies]
postgres = "0.19"
postgres-types = "0.2"
hex = "0.4"
---

use std::env;

use postgres::types::{FromSql, Type};
use postgres::{Config, NoTls};

struct Ewkb<'a>(&'a [u8]);

impl<'a> FromSql<'a> for Ewkb<'a> {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        Ok(Ewkb(raw))
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "geometry"
    }
}

impl<'a> Ewkb<'a> {
    fn srid(&self) -> Option<i32> {
        let data = self.0;
        if data.is_empty() {
            return None;
        }
        let is_be = data[0] == 0;

        let read_i32 = |start: usize| -> Option<i32> {
            if data.len() < start + 4 {
                return None;
            }
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(&data[start..start + 4]);
            Some(if is_be {
                i32::from_be_bytes(bytes)
            } else {
                i32::from_le_bytes(bytes)
            })
        };

        // Read type (u32/i32 compatible for flag checking)
        let type_val = read_i32(1)? as u32;

        if type_val & 0x20000000 != 0 {
            read_i32(5)
        } else {
            None
        }
    }
}

fn main() {
    let mut config = Config::new();

    if let Ok(user) = env::var("PGUSER") {
        config.user(&user);
    }
    if let Ok(host) = env::var("PGHOST") {
        config.host(&host);
    }
    if let Ok(port) = env::var("PGPORT") {
        config.port(port.parse().unwrap());
    }
    if let Ok(password) = env::var("PGPASSWORD") {
        config.password(&password);
    }
    if let Ok(dbname) = env::var("PGDATABASE") {
        config.dbname(&dbname);
    }

    let mut client = config.connect(NoTls).unwrap();

    let sql = "select ST_SetSRID(ST_MakePoint(-87.6298, 41.8781), 4326)";

    // Text mode (Simple Query)
    let rows = client.simple_query(sql).unwrap();
    let row = rows
        .iter()
        .find_map(|msg| match msg {
            postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .expect("Unexpected query message");
    println!("{}", row.get(0).unwrap());

    // Binary mode (Extended Query)
    let rows = client.query(sql, &[]).unwrap();
    let ewkb: Ewkb = rows[0].get(0);
    println!("{}", hex::encode(ewkb.0).to_uppercase());
    println!("SRID: {}", ewkb.srid().unwrap());
}
