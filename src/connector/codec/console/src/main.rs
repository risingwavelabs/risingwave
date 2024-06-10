// Copyright 2024 RisingWave Labs
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

#![feature(panic_update_hook)]

use anyhow::{bail, Context};
use eframe::egui::{self, Color32, RichText};
use egui_extras::syntax_highlighting::CodeTheme;
use itertools::Itertools;
use risingwave_connector_codec::{AvroSchema, JsonSchema, RisingWaveSchema};
use thiserror_ext::AsReport;

#[derive(PartialEq)]
enum Mode {
    Encode,
    Decode,
}
#[derive(PartialEq, Clone, Copy)]
enum Encoding {
    Avro,
    Json,
}

fn parse_and_convert_schema(
    schema_str: &str,
    encoding: Encoding,
) -> anyhow::Result<RisingWaveSchema> {
    match std::panic::catch_unwind(|| {
        match encoding {
            Encoding::Avro => {
                let schema =
                    AvroSchema::parse_str(&schema_str).context("failed to parse avro schema")?;
                // FIXME: not resolved here, Ref will not work
                let column_descs =
                    risingwave_connector_codec::decoder::avro::avro_schema_to_column_descs(
                        &schema, // TODO: support config options
                        None,
                    )?;

                return Ok(column_descs);
            }
            Encoding::Json => {
                let schema = JsonSchema::parse_str(&schema_str)?;
                // TODO: support showing intermediate Avro schema here
                let column_descs =
                    risingwave_connector_codec::decoder::json::json_schema_to_columns(&schema.0)?;
                return Ok(column_descs);
            }
        }
    }) {
        Ok(res) => res,
        Err(_e) => {
            bail!("panicked in parse_and_convert_schema")
        }
    }
}

fn main() -> Result<(), eframe::Error> {
    let window_width = 800.0;
    let window_height = 600.0;
    let options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default().with_inner_size([window_width, window_height]),
        ..Default::default()
    };

    // Our application state:
    let mut theme = ThemeConfig {
        theme: CodeTheme::default(),
        wrap_text: false,
    };

    let mut mode = Mode::Decode;
    let mut encoding = Encoding::Avro;
    let mut input_editor = CodeEditor::new("json", SAMPLE_AVRO_SCHEMA, "Paste your schema here");
    let mut verbose_risingwave_schema = false;

    eframe::run_simple_native("RisingWave Connector Codec", options, move |ctx, _frame| {
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.heading("RisingWave Connector Codec");

            ui.label("Conversion between external schema and RisingWave schema");

            ui.vertical(|ui| {
                ui.label(RichText::new("Mode:").strong());
                ui.radio_value(
                    &mut mode,
                    Mode::Decode,
                    "Decode: external schema -> RisingWave schema (source)",
                );
                ui.radio_value(
                    &mut mode,
                    Mode::Encode,
                    "Encode: RisingWave schema -> external schema (sink)",
                );
            });

            ui.vertical(|ui| {
                ui.label(RichText::new("Encoding:").strong());
                ui.radio_value(&mut encoding, Encoding::Avro, "Apache Avro");
                ui.radio_value(&mut encoding, Encoding::Json, "JSON Schema");
            });

            ui.collapsing("Code Editor Theme", |ui| {
                ui.group(|ui| {
                    ui.checkbox(&mut theme.wrap_text, "wrap text");
                    theme.theme.ui(ui);
                });
            });
        });

        if mode == Mode::Encode {
            egui::CentralPanel::default().show(ctx, |ui| {
                ui.label("not implemented yet. Stay tuned!");
            });
            return;
        }

        egui::SidePanel::left("left_panel")
            .default_width(window_width / 2.0)
            .resizable(false)
            .show(ctx, |ui| {
                ui.heading("Left Panel");
                ui.label("This is the left panel.");

                input_editor.ui(ui, &theme);
            });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.heading("Output RisingWave Schema");

            ui.checkbox(&mut verbose_risingwave_schema, "verbose");

            let schema = input_editor.code.clone();
            match parse_and_convert_schema(&schema, encoding) {
                Ok(risingwave_columns) => {
                    let schema = if verbose_risingwave_schema {
                        format!(
                            "{}",
                            risingwave_columns
                                .into_iter()
                                .format_with(",\n", |column, f| {
                                    f(&format_args!("{:#?}", column))
                                })
                        )
                    } else {
                        format!(
                            "{}",
                            risingwave_columns
                                .into_iter()
                                .format_with("\n", |column, f| {
                                    use risingwave_pb::data::data_type::TypeName;
                                    use risingwave_pb::data::DataType;
                                    fn format_type(data_type: &DataType) -> String {
                                        match data_type.type_name() {
                                            TypeName::List => format!(
                                                "[]{}",
                                                format_type(&data_type.field_type[0])
                                            ),
                                            TypeName::Struct => {
                                                format!(
                                                    "struct<{}>",
                                                    data_type
                                                        .field_names
                                                        .iter()
                                                        .zip_eq(data_type.field_type.iter())
                                                        .format_with(
                                                            ",",
                                                            |(name, data_type), f| f(
                                                                &format_args!(
                                                                    "{name} {}",
                                                                    format_type(data_type)
                                                                )
                                                            )
                                                        )
                                                )
                                            }
                                            simple_type => format!("{simple_type:?}"),
                                        }
                                    }

                                    f(&format_args!(
                                        "name: {}, type: {}",
                                        column.name,
                                        format_type(column.column_type.as_ref().unwrap())
                                    ))
                                })
                        )
                    };

                    egui::ScrollArea::both()
                        .max_width(ui.available_width())
                        .show(ui, |ui| {
                            ui.label(RichText::new(schema).monospace());
                        });
                }
                Err(err) => {
                    ui.label(
                        RichText::new(format!("{}", err.to_report_string_pretty()))
                            .color(Color32::RED)
                            .monospace(),
                    );
                }
            }
        });
    })
}

// ----------------------------------------------------------------------------

struct ThemeConfig {
    theme: CodeTheme,
    wrap_text: bool,
}

pub struct CodeEditor {
    language: String,
    code: String,
    hint: String,
}

impl CodeEditor {
    fn new(language: &str, code: &str, hint: &str) -> Self {
        Self {
            language: language.to_string(),
            code: code.to_string(),
            hint: hint.to_string(),
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, theme: &ThemeConfig) {
        let Self {
            language,
            code,
            hint,
        } = self;

        let mut layouter = |ui: &egui::Ui, string: &str, wrap_width: f32| {
            let mut layout_job = egui_extras::syntax_highlighting::highlight(
                ui.ctx(),
                &theme.theme,
                string,
                language,
            );
            if theme.wrap_text {
                layout_job.wrap.max_width = wrap_width;
            }
            ui.fonts(|f| f.layout_job(layout_job))
        };

        egui::ScrollArea::both().show(ui, |ui| {
            ui.add(
                egui::TextEdit::multiline(code)
                    .font(egui::TextStyle::Monospace) // for cursor height
                    .code_editor()
                    .desired_rows(10)
                    .lock_focus(true)
                    .desired_width(f32::INFINITY)
                    .layouter(&mut layouter)
                    .hint_text(hint.clone()),
            );
        });
    }
}

const SAMPLE_AVRO_SCHEMA: &'static str = r#"{
    "name": "test_student",
    "type": "record",
    "fields": [
      {
        "name": "id",
        "type": "int",
        "default": 0
      },
      {
        "name": "sequence_id",
        "type": "long",
        "default": 0
      },
      {
        "name": "name",
        "type": ["null", "string"]
      },
      {
        "name": "score",
        "type": "float",
        "default": 0.0
      },
      {
        "name": "avg_score",
        "type": "double",
        "default": 0.0
      },
      {
        "name": "is_lasted",
        "type": "boolean",
        "default": false
      },
      {
        "name": "entrance_date",
        "type": "int",
        "logicalType": "date",
        "default": 0
      },
      {
        "name": "birthday",
        "type": "long",
        "logicalType": "timestamp-millis",
        "default": 0
      },
      {
        "name": "anniversary",
        "type": "long",
        "logicalType": "timestamp-micros",
        "default": 0
      },
      {
        "name": "passed",
        "type": {
          "name": "interval",
          "type": "fixed",
          "size": 12
        },
        "logicalType": "duration"
      },
      {
        "name": "bytes",
        "type": "bytes",
        "default": ""
      }
    ]
  }
"#;
