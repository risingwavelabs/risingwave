/*
 * Copyright 2024 RisingWave Labs
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
 */

const PROD_API_ENDPOINT = "/api"
const MOCK_API_ENDPOINT = "http://localhost:32333"
const EXTERNAL_META_NODE_API_ENDPOINT = "http://localhost:5691/api"

export const PREDEFINED_API_ENDPOINTS = [
  PROD_API_ENDPOINT,
  MOCK_API_ENDPOINT,
  EXTERNAL_META_NODE_API_ENDPOINT,
]

export const DEFAULT_API_ENDPOINT: string =
  process.env.NODE_ENV === "production"
    ? PROD_API_ENDPOINT
    : EXTERNAL_META_NODE_API_ENDPOINT // EXTERNAL_META_NODE_API_ENDPOINT to debug with RisingWave servers

export const API_ENDPOINT_KEY = "risingwave.dashboard.api.endpoint"

class Api {
  urlFor(path: string) {
    let apiEndpoint: string = (
      JSON.parse(localStorage.getItem(API_ENDPOINT_KEY) || "null") ||
      DEFAULT_API_ENDPOINT
    ).replace(/\/+$/, "") // remove trailing slashes

    return `${apiEndpoint}${path}`
  }

  async get(path: string) {
    const url = this.urlFor(path)

    try {
      const res = await fetch(url)
      const data = await res.json()

      // Throw error if response is not ok.
      // See `DashboardError::into_response`.
      if (!res.ok) {
        throw `${res.status} ${res.statusText}${
          data.error ? ": " + data.error : ""
        }`
      }
      return data
    } catch (e) {
      console.error(e)
      throw Error(`Failed to fetch ${url}`, {
        cause: e,
      })
    }
  }
}

const api = new Api()
export default api
