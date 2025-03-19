// Copyright 2025 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use super::*;

pub fn check_nexmark_schema(
    props: &WithOptionsSecResolved,
    row_id_index: Option<usize>,
    columns: &[ColumnCatalog],
) -> Result<()> {
    let table_type = props
        .get("nexmark.table.type")
        .map(|t| t.to_ascii_lowercase());

    let event_type = match table_type.as_deref() {
        None => None,
        Some("bid") => Some(EventType::Bid),
        Some("auction") => Some(EventType::Auction),
        Some("person") => Some(EventType::Person),
        Some(t) => {
            return Err(RwError::from(ProtocolError(format!(
                "unsupported table type for nexmark source: {}",
                t
            ))));
        }
    };

    // Ignore the generated columns and map the index of row_id column.
    let user_defined_columns = columns.iter().filter(|c| !c.is_generated());
    let row_id_index = if let Some(index) = row_id_index {
        let col_id = columns[index].column_id();
        user_defined_columns
            .clone()
            .position(|c| c.column_id() == col_id)
            .unwrap()
            .into()
    } else {
        None
    };

    let expected = get_event_data_types_with_names(event_type, row_id_index);
    let user_defined = user_defined_columns
        .map(|c| {
            (
                c.column_desc.name.to_ascii_lowercase(),
                c.column_desc.data_type.to_owned(),
            )
        })
        .collect_vec();

    let schema_eq = expected.len() == user_defined.len()
        && expected
            .iter()
            .zip_eq_fast(user_defined.iter())
            .all(|((name1, type1), (name2, type2))| name1 == name2 && type1.equals_datatype(type2));

    if !schema_eq {
        let cmp = pretty_assertions::Comparison::new(&expected, &user_defined);
        return Err(RwError::from(ProtocolError(format!(
            "The schema of the nexmark source must specify all columns in order:\n{cmp}",
        ))));
    }
    Ok(())
}
