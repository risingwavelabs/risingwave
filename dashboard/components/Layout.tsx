/*
 * Copyright 2023 RisingWave Labs
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

import {
  Box,
  Button,
  Flex,
  HStack,
  Image,
  Text,
  VStack,
} from "@chakra-ui/react"
import Link from "next/link"
import { useRouter } from "next/router"
import React, { useEffect, useState } from "react"
import { UrlObject } from "url"
import {
  IconArrowRightCircle,
  IconArrowRightCircleFill,
  IconServer,
} from "../components/utils/icons"

export const NAVBAR_WIDTH = "300px"

function NavButton({
  href,
  children,
  leftIcon,
  leftIconActive,
}: {
  href: string | UrlObject
  children?: React.ReactNode
  leftIcon?: React.ReactElement
  leftIconActive?: React.ReactElement
}) {
  const router = useRouter()
  const [match, setMatch] = useState(false)

  useEffect(() => {
    setMatch(router.asPath.startsWith(href.toString()))
    return () => {}
  }, [href, router.asPath])

  return (
    <Link href={href}>
      <Button
        colorScheme={match ? "teal" : "gray"}
        color={match ? "teal.600" : "gray.500"}
        variant={match ? "outline" : "ghost"}
        width="full"
        justifyContent="flex-start"
        leftIcon={
          match
            ? leftIconActive || leftIcon || <IconArrowRightCircleFill />
            : leftIcon || <IconArrowRightCircle />
        }
      >
        {children}
      </Button>
    </Link>
  )
}

function NavTitle({ children }: { children: React.ReactNode }) {
  return (
    <Text mt={3} textColor="teal.500" fontWeight="semibold" lineHeight="6">
      {children}
    </Text>
  )
}

function Layout({ children }: { children: React.ReactNode }) {
  return (
    <Flex>
      <Box
        height="100vh"
        overflowY="scroll"
        width={NAVBAR_WIDTH}
        minWidth={NAVBAR_WIDTH}
        bg="gray.50"
        py={3}
        px={3}
      >
        <VStack>
          <Box height="50px" width="full">
            <HStack spacing={0}>
              <Link href="/" passHref>
                <a>
                  <Image
                    boxSize="50px"
                    src="/risingwave.svg"
                    alt="RisingWave Logo"
                  />
                </a>
              </Link>
              <Text fontSize="xl">
                <b>RisingWave</b> Dashboard
              </Text>
            </HStack>
          </Box>
          <NavButton href="/cluster/" leftIcon={<IconServer />}>
            Cluster Overview
          </NavButton>
          <VStack width="full" alignItems="flex-start" px={3}>
            <NavTitle>Catalog</NavTitle>
            <NavButton href="/data_sources/">Data Sources</NavButton>
            <NavButton href="/tables/">Tables</NavButton>
            <NavButton href="/materialized_views/">
              Materialized Views
            </NavButton>
            <NavButton href="/indexes/">Indexes</NavButton>
            <NavButton href="/internal_tables/">Internal Tables</NavButton>
            <NavButton href="/sinks/">Sinks</NavButton>
          </VStack>
          <VStack width="full" alignItems="flex-start" px={3}>
            <NavTitle>Streaming</NavTitle>
            <NavButton href="/streaming_graph/">Graph</NavButton>
            <NavButton href="/streaming_plan/">Fragments</NavButton>
          </VStack>
          <VStack width="full" alignItems="flex-start" px={3}>
            <NavTitle>Batch</NavTitle>
            <NavButton href="/batch_tasks/">Batch Tasks</NavButton>
          </VStack>
          <VStack width="full" alignItems="flex-start" px={3}>
            <NavTitle>Explain</NavTitle>
            <NavButton href="/explain_distsql/">Distributed Plan</NavButton>
          </VStack>
          <VStack width="full" alignItems="flex-start" px={3}>
            <NavTitle>Debug</NavTitle>
            <NavButton href="/await_tree/">Await Tree Dump</NavButton>
          </VStack>
          <VStack mb={3}></VStack>
          <NavButton href="/settings/">Settings</NavButton>
        </VStack>
      </Box>
      <Box flex={1} overflowY="scroll">
        {children}
      </Box>
    </Flex>
  )
}

export default Layout
