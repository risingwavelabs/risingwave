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
  IconBoxArrowUpRight,
  IconServer,
} from "../components/utils/icons"

export const NAVBAR_WIDTH = "300px"

function NavButton({
  href,
  children,
  leftIcon,
  leftIconActive,
  external,
}: {
  href: string | UrlObject
  children?: React.ReactNode
  leftIcon?: React.ReactElement
  leftIconActive?: React.ReactElement
  external?: boolean
}) {
  const router = useRouter()
  const [match, setMatch] = useState(false)

  useEffect(() => {
    setMatch(router.asPath.startsWith(href.toString()))
    return () => {}
  }, [href, router.asPath])

  const icon =
    leftIcon || (external ? <IconBoxArrowUpRight /> : <IconArrowRightCircle />)
  const activeIcon =
    leftIconActive ||
    leftIcon ||
    (external ? undefined : <IconArrowRightCircleFill />)

  return (
    <Link href={href} target={external ? "_blank" : undefined}>
      <Button
        colorScheme={match ? "blue" : "gray"}
        color={match ? "blue.600" : "gray.500"}
        variant={match ? "outline" : "ghost"}
        width="full"
        justifyContent="flex-start"
        leftIcon={match ? activeIcon : icon}
      >
        {children}
      </Button>
    </Link>
  )
}

function NavTitle({ children }: { children: React.ReactNode }) {
  return (
    <Text mt={3} textColor="blue.500" fontWeight="semibold" lineHeight="6">
      {children}
    </Text>
  )
}

function Section({ children }: { children: React.ReactNode }) {
  return (
    <VStack width="full" alignItems="flex-start" px={3}>
      {children}
    </VStack>
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
                <Image
                  boxSize="50px"
                  src="/risingwave.svg"
                  alt="RisingWave Logo"
                />
              </Link>
              <Text fontSize="xl">
                <b>RisingWave</b> Dashboard
              </Text>
            </HStack>
          </Box>
          <Section>
            <NavButton href="/cluster/" leftIcon={<IconServer />}>
              Cluster Overview
            </NavButton>
          </Section>
          <Section>
            <NavTitle>Catalog</NavTitle>
            <NavButton href="/sources/">Sources</NavButton>
            <NavButton href="/tables/">Tables</NavButton>
            <NavButton href="/materialized_views/">
              Materialized Views
            </NavButton>
            <NavButton href="/indexes/">Indexes</NavButton>
            <NavButton href="/internal_tables/">Internal Tables</NavButton>
            <NavButton href="/sinks/">Sinks</NavButton>
            <NavButton href="/views/">Views</NavButton>
            <NavButton href="/subscriptions/">Subscriptions</NavButton>
            <NavButton href="/functions/">Functions</NavButton>
          </Section>
          <Section>
            <NavTitle>Streaming</NavTitle>
            <NavButton href="/relation_graph/">Relation Graph</NavButton>
            <NavButton href="/fragment_graph/">Fragment Graph</NavButton>
          </Section>
          <Section>
            <NavTitle>Batch</NavTitle>
            <NavButton href="/batch_tasks/">Batch Tasks</NavButton>
          </Section>
          <Section>
            <NavTitle>Explain</NavTitle>
            <NavButton href="/explain_distsql/">Distributed Plan</NavButton>
          </Section>
          <Section>
            <NavTitle>Debug</NavTitle>
            <NavButton href="/await_tree/">Await Tree Dump</NavButton>
            <NavButton href="/heap_profiling/">Heap Profiling</NavButton>
            <NavButton href="/api/monitor/diagnose" external>
              Diagnose
            </NavButton>
            <NavButton href="/trace/search" external>
              Traces
            </NavButton>
          </Section>
          <Section>
            <NavTitle>Settings</NavTitle>
            <NavButton href="/settings/">Settings</NavButton>
          </Section>
        </VStack>
      </Box>
      <Box flex={1} overflowY="scroll">
        {children}
      </Box>
    </Flex>
  )
}

export default Layout
