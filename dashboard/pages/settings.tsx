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

import { Box, FormControl, FormLabel, Input, VStack } from "@chakra-ui/react"
import { useIsClient, useLocalStorage } from "@uidotdev/usehooks"
import Head from "next/head"
import { Fragment } from "react"
import Title from "../components/Title"
import {
  API_ENDPOINT_KEY,
  DEFAULT_API_ENDPOINT,
  PREDEFINED_API_ENDPOINTS,
} from "../lib/api/api"

export default function Settings() {
  const isClient = useIsClient()
  return isClient && <ClientSettings />
}

// Local storage is only available on the client side.
function ClientSettings() {
  const [apiEndpoint, saveApiEndpoint] = useLocalStorage(
    API_ENDPOINT_KEY,
    DEFAULT_API_ENDPOINT
  )

  return (
    <Fragment>
      <Head>
        <title>Settings</title>
      </Head>
      <Box p={3}>
        <Title>Settings</Title>
        <VStack spacing={4} w="full">
          <FormControl>
            <FormLabel>RisingWave Meta Node HTTP API</FormLabel>
            <Input
              value={apiEndpoint}
              onChange={(event) => saveApiEndpoint(event.target.value)}
              list="predefined"
            />
            <datalist id="predefined">
              {PREDEFINED_API_ENDPOINTS.map((endpoint) => (
                <option key={endpoint} value={endpoint} />
              ))}
            </datalist>
          </FormControl>
        </VStack>
      </Box>
    </Fragment>
  )
}
