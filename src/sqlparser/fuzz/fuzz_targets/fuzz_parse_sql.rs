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

use honggfuzz::fuzz;
use risingwave_sqlparser::dialect::GenericDialect;
use risingwave_sqlparser::parser::Parser;

fn main() {
    loop {
        fuzz!(|data: String| {
            let dialect = GenericDialect {};
            let _ = Parser::parse_sql(&dialect, &data);
        });
    }
}
