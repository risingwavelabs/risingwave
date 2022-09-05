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

import { Box, FormControl, FormLabel, Input, VStack } from "@chakra-ui/react"
import Title from "../components/Title"

export default function Settings() {
  return (
    <Box p={3}>
      <Title>Settings</Title>
      <VStack spacing={4} w="full">
        <FormControl>
          <FormLabel>RisingWave Meta Node HTTP API</FormLabel>
          <Input value="/api" />
        </FormControl>
        <FormControl>
          <FormLabel>Grafana HTTP API</FormLabel>
          <Input value="/api" />
        </FormControl>
        <FormControl>
          <FormLabel>Prometheus HTTP API</FormLabel>
          <Input value="/api" />
        </FormControl>
      </VStack>
    </Box>
  )
}
