/*
 * Copyright 2025 RisingWave Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

import { Column, Relations } from "../components/Relations"
import { getFunctions } from "../lib/api/streaming"
import { Function } from "../proto/gen/catalog"

const argNamesColumn: Column<Function> = {
  name: "Arg Names",
  width: 3,
  content: (s) => s.argNames.join(", "),
}

const argTypesColumn: Column<Function> = {
  name: "Arg Types",
  width: 3,
  content: (s) => s.argTypes.map((t) => t.typeName).join(", "),
}

const returnTypeColumn: Column<Function> = {
  name: "Return Type",
  width: 3,
  content: (s) => s.returnType?.typeName ?? "unknown",
}

const languageColumn: Column<Function> = {
  name: "Language",
  width: 3,
  content: (s) => s.language,
}

const kindColumn: Column<Function> = {
  name: "Kind",
  width: 3,
  content: (s) => s.kind?.$case,
}

export default function Functions() {
  return Relations("Functions", getFunctions, [
    argNamesColumn,
    argTypesColumn,
    returnTypeColumn,
    languageColumn,
    kindColumn,
  ])
}
