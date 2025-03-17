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

const express = require("express")
const cors = require("cors")

const app = express()
app.use(cors())

app.listen(32333, () => {
  console.log("Server running on port 32333")
})

app.get("/actors", (req, res, next) => {
  res.json(require("./mock/actors.json"))
})

app.get("/fragments2", (req, res, next) => {
  res.json(require("./mock/fragments2.json"))
})

app.get("/materialized_views", (req, res, next) => {
  res.json(require("./mock/materialized_views.json"))
})

app.get("/tables", (req, res, next) => {
  res.json(require("./mock/tables.json"))
})

app.get("/indexes", (req, res, next) => {
  res.json(require("./mock/indexes.json"))
})

app.get("/indexes", (req, res, next) => {
  res.json(require("./mock/indexes.json"))
})

app.get("/internal_tables", (req, res, next) => {
  res.json(require("./mock/internal_tables.json"))
})

app.get("/sinks", (req, res, next) => {
  res.json(require("./mock/sinks.json"))
})

app.get("/sources", (req, res, next) => {
  res.json(require("./mock/sources.json"))
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

app.get("/metrics/cluster", (req, res, next) => {
  res.json(require("./mock/metrics_cluster.json"))
})

app.get("/monitor/await_tree/1", (req, res, next) => {
  res.json(require("./mock/await_tree_1.json"))
})
