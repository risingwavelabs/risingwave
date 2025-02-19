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

use std::fs;

use risingwave_common::catalog::ColumnDesc;
use risingwave_connector_codec::JsonSchema;
use url::Url;

use crate::utils::{ColumnDescTestDisplay, *};

#[tokio::test]
async fn test_json_schema_parse() {
    let test_id_type_file_path = fs::canonicalize("tests/test_data/id_type.json")
        .unwrap()
        .to_string_lossy()
        .to_string();
    let test_id_type_http_url = "https://gist.githubusercontent.com/yuhao-su/a1b23e4073b4f1ca4e614c89a785575d/raw/ec8ccd6b3bcf6fafe5c57173a4fdf4129c63625d/idType.txt";

    let schema = format!(
        r##" {{
          "$schema": "http://json-schema.org/draft-07/schema#",
          "type": "object",
          "definitions": {{
            "stringType": {{
              "type": "string"
            }},
            "marketObj": {{
              "type": "object",
              "additionalProperties": {{
                "$ref": "{test_id_type_http_url}#/definitions/idType"
              }}
            }},
            "marketArray": {{
              "type": "object",
              "additionalProperties": {{
                "type": "array",
                "items": {{
                  "type": "string"
                }}
              }}
            }},
            "recurrentType": {{
              "type": "object",
              "properties": {{
                "id": {{
                  "$ref": "file://{test_id_type_file_path}#/definitions/idType"
                }},
                "next": {{
                  "$ref": "file://{test_id_type_file_path}#/definitions/recurrentType"
                }}
              }}
            }}
          }},
          "properties": {{
            "id": {{
              "$ref": "file://{test_id_type_file_path}#/definitions/idType"
            }},
            "name": {{
              "$ref": "#/definitions/marketObj",
              "description": "Name of the market subject"
            }},
            "cats": {{
              "$ref": "#/definitions/marketArray"
            }},
            "meta": {{
              "type": "object",
              "properties": {{
                "active": {{
                  "$ref": "#/definitions/marketObj"
                }},
                "tags": {{
                  "$ref": "#/definitions/marketArray"
                }}
              }}
            }},
            "recurrent": {{
              "$ref": "#/definitions/recurrentType"
            }}
          }},
          "required": [
            "id",
            "name"
          ]
        }}"##
    );

    let mut json_schema = JsonSchema::parse_str(&schema).unwrap();

    let columns = json_schema
        .json_schema_to_columns(Url::parse("http://test_schema_uri.test").unwrap())
        .await
        .unwrap()
        .into_iter()
        .map(ColumnDesc::from)
        .collect_vec();
    let column_display = columns.iter().map(ColumnDescTestDisplay).collect_vec();

    expect![[r#"
        {
          "$schema": "http://json-schema.org/draft-07/schema#",
          "definitions": {
            "marketArray": {
              "additionalProperties": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "type": "object"
            },
            "marketObj": {
              "additionalProperties": {
                "type": "string"
              },
              "type": "object"
            },
            "recurrentType": {
              "properties": {
                "id": {
                  "type": "string"
                },
                "next": {
                  "properties": {
                    "id": {
                      "type": "string"
                    },
                    "next": {}
                  },
                  "type": "object"
                }
              },
              "type": "object"
            },
            "stringType": {
              "type": "string"
            }
          },
          "properties": {
            "cats": {
              "additionalProperties": {
                "items": {
                  "type": "string"
                },
                "type": "array"
              },
              "type": "object"
            },
            "id": {
              "type": "string"
            },
            "meta": {
              "properties": {
                "active": {
                  "additionalProperties": {
                    "type": "string"
                  },
                  "type": "object"
                },
                "tags": {
                  "additionalProperties": {
                    "items": {
                      "type": "string"
                    },
                    "type": "array"
                  },
                  "type": "object"
                }
              },
              "type": "object"
            },
            "name": {
              "additionalProperties": {
                "type": "string"
              },
              "type": "object"
            },
            "recurrent": {
              "properties": {
                "id": {
                  "type": "string"
                },
                "next": {
                  "properties": {
                    "id": {
                      "type": "string"
                    },
                    "next": {}
                  },
                  "type": "object"
                }
              },
              "type": "object"
            }
          },
          "required": [
            "id",
            "name"
          ],
          "type": "object"
        }"#]]
    .assert_eq(&serde_json::to_string_pretty(&json_schema.0).unwrap());

    expect![[r#"
        [
            cats(#1): Jsonb,
            id(#2): Varchar,
            meta(#3): Struct {
                active: Jsonb,
                tags: Jsonb,
            },
            name(#4): Jsonb,
            recurrent(#5): Struct {
                id: Varchar,
                next: Struct {
                    id: Varchar,
                    next: Varchar,
                },
            },
        ]
    "#]]
    .assert_debug_eq(&column_display);
}
