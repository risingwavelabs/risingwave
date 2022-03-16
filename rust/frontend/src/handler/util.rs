use itertools::Itertools;
use pgwire::pg_field_descriptor::{PgFieldDescriptor, TypeOid};
use pgwire::types::Row;
use risingwave_common::array::DataChunk;
use risingwave_common::error::{ErrorCode, Result};
use risingwave_common::types::DataType;

use crate::binder::{BoundSetExpr, BoundStatement};
use crate::expr::Expr;

pub fn to_pg_rows(chunk: DataChunk) -> Vec<Row> {
    chunk
        .rows()
        .map(|r| {
            Row::new(
                r.0.into_iter()
                    .map(|data| data.map(|d| d.to_string()))
                    .collect_vec(),
            )
        })
        .collect_vec()
}

pub fn get_pg_field_descs(bound: BoundStatement) -> Result<Vec<PgFieldDescriptor>> {
    if let BoundStatement::Query(query) = bound {
        if let BoundSetExpr::Select(select) = query.body {
            let mut pg_descs = vec![];
            for i in 0..select.select_items.len() {
                pg_descs.push(PgFieldDescriptor::new(
                    select.aliases[i].as_ref().unwrap().to_string(),
                    data_type_to_type_oid(select.select_items[i].return_type()),
                ));
            }
            Ok(pg_descs)
        } else {
            Err(ErrorCode::NotImplementedError(
                "get pg_field descs only support select bound_set_expr".to_string(),
            )
            .into())
        }
    } else {
        Err(ErrorCode::NotImplementedError(
            "get pg_field descs only support query bound_statement".to_string(),
        )
        .into())
    }
}

pub fn data_type_to_type_oid(data_type: DataType) -> TypeOid {
    match data_type {
        DataType::Int16 => TypeOid::SmallInt,
        DataType::Int32 => TypeOid::Int,
        DataType::Int64 => TypeOid::BigInt,
        DataType::Float32 => TypeOid::Float4,
        DataType::Float64 => TypeOid::Float8,
        DataType::Boolean => TypeOid::Boolean,
        DataType::Char => TypeOid::CharArray,
        DataType::Varchar => TypeOid::Varchar,
        DataType::Date => TypeOid::Date,
        DataType::Time => TypeOid::Time,
        DataType::Timestamp => TypeOid::Timestamp,
        DataType::Timestampz => TypeOid::Timestampz,
        DataType::Decimal => TypeOid::Decimal,
        DataType::Interval => TypeOid::Varchar,
        DataType::Struct { .. } => TypeOid::Varchar,
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use pgwire::pg_field_descriptor::TypeOid;
    use risingwave_common::array::*;
    use risingwave_common::{column, column_nonnull};

    use crate::binder::{BoundQuery, BoundSelect, BoundSetExpr, BoundStatement};
    use crate::expr::ExprImpl;
    use crate::handler::util::{get_pg_field_descs, to_pg_rows};

    #[test]
    fn test_get_pg_field_descs() {
        let select = BoundSelect {
            distinct: false,
            select_items: vec![
                ExprImpl::literal_int(1),
                ExprImpl::literal_int(2),
                ExprImpl::literal_bool(true),
            ],
            aliases: vec![
                Some("column1".to_string()),
                Some("column2".to_string()),
                Some("column3".to_string()),
            ],
            from: None,
            selection: None,
        };
        let bound = BoundStatement::Query(
            BoundQuery {
                body: BoundSetExpr::Select(select.into()),
                order: vec![],
            }
            .into(),
        );
        let pg_descs = get_pg_field_descs(bound).unwrap();
        assert_eq!(
            pg_descs
                .clone()
                .into_iter()
                .map(|p| { p.get_name().to_string() })
                .collect_vec(),
            [
                "column1".to_string(),
                "column2".to_string(),
                "column3".to_string()
            ]
        );
        assert_eq!(
            pg_descs
                .into_iter()
                .map(|p| { p.get_type_oid().as_number() })
                .collect_vec(),
            [
                TypeOid::Int.as_number(),
                TypeOid::Int.as_number(),
                TypeOid::Boolean.as_number()
            ]
        );
    }

    #[test]
    fn test_to_pg_rows() {
        let chunk = DataChunk::new(
            vec![
                column_nonnull!(I32Array, [1, 2, 3, 4]),
                column!(I64Array, [Some(6), None, Some(7), None]),
                column!(F32Array, [Some(6.01), None, Some(7.01), None]),
                column!(Utf8Array, [Some("aaa"), None, Some("vvv"), None]),
            ],
            None,
        );
        let rows = to_pg_rows(chunk);
        let expected = vec![
            vec![
                Some("1".to_string()),
                Some("6".to_string()),
                Some("6.01".to_string()),
                Some("aaa".to_string()),
            ],
            vec![Some("2".to_string()), None, None, None],
            vec![
                Some("3".to_string()),
                Some("7".to_string()),
                Some("7.01".to_string()),
                Some("vvv".to_string()),
            ],
            vec![Some("4".to_string()), None, None, None],
        ];
        let vec = rows
            .into_iter()
            .map(|r| r.values().iter().cloned().collect_vec())
            .collect_vec();

        assert_eq!(vec, expected);
    }
}
