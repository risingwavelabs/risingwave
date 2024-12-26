use anyhow::anyhow;
use risingwave_common::types::DataType;
use risingwave_sqlparser::ast::{DataType as AstDataType, StructField};

use crate::error::Result;

pub fn to_ast_data_type(ty: &DataType) -> Result<AstDataType> {
    match ty {
        DataType::Boolean => Ok(AstDataType::Boolean),
        DataType::Int16 => Ok(AstDataType::SmallInt),
        DataType::Int32 => Ok(AstDataType::Int),
        DataType::Int64 => Ok(AstDataType::BigInt),
        DataType::Float32 => Ok(AstDataType::Real),
        DataType::Float64 => Ok(AstDataType::Double),
        // TODO: handle precision and scale for decimal
        DataType::Decimal => Ok(AstDataType::Decimal(None, None)),
        DataType::Date => Ok(AstDataType::Date),
        DataType::Varchar => Ok(AstDataType::Varchar),
        DataType::Time => Ok(AstDataType::Time(false)),
        DataType::Timestamp => Ok(AstDataType::Timestamp(false)),
        DataType::Timestamptz => Ok(AstDataType::Timestamp(true)),
        DataType::Interval => Ok(AstDataType::Interval),
        DataType::Jsonb => Ok(AstDataType::Jsonb),
        DataType::Bytea => Ok(AstDataType::Bytea),
        DataType::List(item_ty) => Ok(AstDataType::Array(Box::new(to_ast_data_type(item_ty)?))),
        DataType::Struct(fields) => {
            let fields = fields
                .iter()
                .map(|(name, ty)| {
                    to_ast_data_type(ty).map(|ty| StructField {
                        name: name.into(),
                        data_type: ty,
                    })
                    // Result::Ok(StructField {
                    //     name: name.into(),
                    //     data_type: to_ast_data_type(ty)?,
                    // })
                })
                .try_collect()?;
            Ok(AstDataType::Struct(fields))
        }
        DataType::Serial | DataType::Int256 | DataType::Map(_) => {
            // TODO: support them
            Err(anyhow!("unsupported data type: {:?}", ty).context("to_ast_data_type"))?
        }
    }
}
