import { Box, HStack, Image, Text, VStack, Button } from '@chakra-ui/react';
import Link from 'next/link';
import React, { Fragment } from 'react';
import { IconArrowRightCircle, IconServer } from '../components/utils/icons';


function Layout({ children }: { children: React.ReactNode }) {
    return <Fragment>
        <Box position="fixed" top={0} bottom={0} left={0} width="320px" bg="gray.50" py={3} px={3}>
            <Box height="50px" width="100%" mb={3}>
                <HStack>
                    <Image boxSize="50px" src="/risingwave.svg" />
                    <Text fontSize="xl"><b>RisingWave</b> Dashboard</Text>
                </HStack>
            </Box>
            <VStack>
                <Link href="/cluster">
                    <Button colorScheme='blue' variant='ghost' width="100%" justifyContent="start" leftIcon={<IconServer />}>
                        Cluster
                    </Button>
                </Link>
                <Link href="/streaming">
                    <Button colorScheme='blue' variant='ghost' width="100%" justifyContent="start" leftIcon={<IconArrowRightCircle />}>
                        Streaming
                    </Button>
                </Link>
                <Link href="/batch">
                    <Button colorScheme='blue' variant='ghost' width="100%" justifyContent="start" leftIcon={<IconArrowRightCircle />} >
                        Batch
                    </Button>
                </Link>
            </VStack>
        </Box>
        <Box ml="320px" width="calc(100vw - 320px)" height="100%">
            {children}
        </Box>
    </Fragment>
}

export default Layout