import { Box, Button, HStack, Image, Text, VStack } from "@chakra-ui/react"
import Link from "next/link"
import { useRouter } from "next/router"
import React, { Fragment } from "react"
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
  const match = router.asPath.startsWith(href.toString())
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

function Layout({ children }: { children: React.ReactNode }) {
  return (
    <Fragment>
      <Box
        position="fixed"
        top={0}
        bottom={0}
        left={0}
        width={NAVBAR_WIDTH}
        bg="gray.50"
        py={3}
        px={3}
      >
        <Box height="50px" width="full" mb={3}>
          <HStack spacing={0}>
            <Image boxSize="50px" src="/risingwave.svg" />
            <Text fontSize="xl">
              <b>RisingWave</b> Dashboard
            </Text>
          </HStack>
        </Box>
        <VStack>
          <NavButton href="/cluster/" leftIcon={<IconServer />}>
            Cluster Overview
          </NavButton>
          <NavButton href="/data_sources/">Data Sources</NavButton>
          <NavButton href="/materialized_views/">Materialized Views</NavButton>
          <NavButton href="/streaming_plan/">Streaming Plan</NavButton>
          <NavButton href="/batch_tasks/">Batch Tasks</NavButton>
          <NavButton href="/settings/">Settings</NavButton>
        </VStack>
      </Box>
      <Box
        ml={NAVBAR_WIDTH}
        width={`calc(100vw - ${NAVBAR_WIDTH})`}
        height="full"
      >
        {children}
      </Box>
    </Fragment>
  )
}

export default Layout
