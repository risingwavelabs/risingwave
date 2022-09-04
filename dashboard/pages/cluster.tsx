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

import { Box, Grid, GridItem, HStack, Text, useToast, VStack } from "@chakra-ui/react";
import { Fragment, useEffect, useState } from "react";
import Title from "../components/Title";
import { WorkerNode } from "../proto/gen/common";
import { getClusterInfoComputeNode, getClusterInfoFrontend } from "./api/cluster";

function WorkerNodeComponent({ workerNodeType, workerNode }: { workerNodeType: string, workerNode: WorkerNode }) {
    return <Fragment>
        <VStack alignItems="start" spacing={1}>
            <HStack>
                <Box w={3} h={3} flex="none" bgColor="green.600" rounded="full"></Box>
                <Text fontWeight="medium" fontSize="xl" textColor="black">{workerNodeType} #{workerNode.id}</Text>
            </HStack>
            <Text textColor="gray.500" m={0}>Running</Text>
            <Text textColor="gray.500" m={0}>{workerNode.host?.host}:{workerNode.host?.port}</Text>
        </VStack>
    </Fragment>
}
export default function Cluster() {
    const [frontendList, setFrontendList] = useState<WorkerNode[]>([]);
    const [computeNodeList, setComputeNodeList] = useState<WorkerNode[]>([]);
    const toast = useToast()

    useEffect(() => {
        async function doFetch() {
            try {
                setFrontendList(await getClusterInfoFrontend());
                setComputeNodeList(await getClusterInfoComputeNode());
            } catch (e: any) {
                toast({
                    title: 'Error Occurred',
                    description: e.toString(),
                    status: 'error',
                    duration: 5000,
                    isClosable: true,
                })
                console.error(e);
            }
        }
        doFetch();
        return () => { }
    }, []);

    return (
        <Box p={3}>
            <Title>Cluster Information</Title>
            <Grid templateColumns='repeat(3, 1fr)' gap={6} width="full">
                {frontendList.map((frontend) => <GridItem w='full' rounded="xl" bg="white" shadow="md" p={6} key={frontend.id}>
                    <WorkerNodeComponent workerNodeType="Frontend" workerNode={frontend} />
                </GridItem>
                )}
                {computeNodeList.map((computeNode) => <GridItem w='full' rounded="xl" bg="white" shadow="md" p={6} key={computeNode.id}>
                    <WorkerNodeComponent workerNodeType="Compute" workerNode={computeNode} />
                </GridItem>
                )}
            </Grid>
        </Box>
    );
}