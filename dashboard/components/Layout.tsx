import { Box, HStack, Image, Text, VStack, Button } from '@chakra-ui/react';
import Link from 'next/link';
import { useRouter } from 'next/router';
import React, { Fragment } from 'react';
import { UrlObject } from 'url';
import { IconArrowRightCircle, IconArrowRightCircleFill, IconServer } from '../components/utils/icons';

function NavButton({ href, children, leftIcon, leftIconActive }: {
    href: string | UrlObject,
    children?: React.ReactNode,
    leftIcon?: React.ReactElement,
    leftIconActive?: React.ReactElement
}) {
    const router = useRouter()
    const match = router.asPath.startsWith(href.toString())
    return <Link href={href}>
        <Button
            colorScheme={match ? "blue" : "gray"}
            color={match ? "blue.600" : "gray.500"}
            variant={match ? "outline" : "ghost"}
            width="full"
            justifyContent="flex-start"
            leftIcon={match ?
                (leftIconActive || leftIcon || <IconArrowRightCircleFill />) :
                (leftIcon || <IconArrowRightCircle />)}
        >
            {children}
        </Button>
    </Link>
}

function Layout({ children }: { children: React.ReactNode }) {
    return <Fragment>
        <Box position="fixed" top={0} bottom={0} left={0} width="320px" bg="gray.50" py={3} px={3}>
            <Box height="50px" width="full" mb={3}>
                <HStack>
                    <Image boxSize="50px" src="/risingwave.svg" />
                    <Text fontSize="xl"><b>RisingWave</b> Dashboard</Text>
                </HStack>
            </Box>
            <VStack>
                <NavButton href="/cluster/" leftIcon={<IconServer />}>Cluster</NavButton>
                <NavButton href="/streaming/">Streaming</NavButton>
                <NavButton href="/batch/">Batch</NavButton>
                <NavButton href="/settings/">Settings</NavButton>
            </VStack>
        </Box>
        <Box ml="320px" width="calc(100vw - 320px)" height="full">
            {children}
        </Box>
    </Fragment>
}

export default Layout