/*
 * Copyright 2022 Singularity Data
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

const express = require("express")

const app = express()
app.listen(32333, () => {
  console.log("Server running on port 32333")
})

app.get("/actors", (req, res, next) => {
  res.json(require("./mock/actors.json"))
})

app.get("/fragments", (req, res, next) => {
  res.json(require("./mock/fragments.json"))
})

app.get("/materialized_views", (req, res, next) => {
  res.json(require("./mock/materialized_views.json"))
})

app.get("/clusters/0", (req, res, next) => {
  res.json(require("./mock/cluster_0.json"))
})

app.get("/clusters/1", (req, res, next) => {
  res.json(require("./mock/cluster_1.json"))
})

app.get("/clusters/2", (req, res, next) => {
  res.json(require("./mock/cluster_2.json"))
})

app.get("/sources/", (req, res, next) => {
  res.json(require("./mock/sources.json"))
})
