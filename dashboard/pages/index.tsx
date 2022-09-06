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

import { Flex, HStack, Image, Text, VStack } from "@chakra-ui/react"
import Head from "next/head"
import { Fragment } from "react"
import { NAVBAR_WIDTH } from "../components/Layout"
import Title from "../components/Title"

export default function Home() {
  return (
    <Fragment>
      <Head>
        <title>RisingWave Dashboard</title>
      </Head>
      <Flex
        w={`calc(100vw - ${NAVBAR_WIDTH})`}
        alignItems="center"
        justifyContent="center"
        position="fixed"
        top={0}
        bottom={0}
      >
        <VStack spacing={3}>
          <HStack spacing={2}>
            <Flex flexDirection="column" width="md" alignItems="end">
              <Image boxSize="sm" src="/risingwave.svg" alt="RisingWave Logo" />
            </Flex>
            <VStack alignItems="start" width="md">
              <Text fontSize="2xl">Welcome to</Text>
              <Text fontSize="3xl">
                <b>RisingWave</b> Dashboard
              </Text>
            </VStack>
          </HStack>
          <Title>Click the sidebar to continue.</Title>
        </VStack>
      </Flex>
    </Fragment>
  )
}
