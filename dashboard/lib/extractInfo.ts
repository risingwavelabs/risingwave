import { ColumnCatalog } from "../proto/gen/plan_common"

export default function extractColumnInfo(col: ColumnCatalog) {
  return `${col.columnDesc?.name} (${col.columnDesc?.columnType?.typeName})`
}
