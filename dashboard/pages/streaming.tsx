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

import { Box, Text, useToast } from "@chakra-ui/react";
import { useEffect, useState } from "react";
import Title from "../components/Title";
import { Table } from "../proto/gen/catalog";
import { ActorLocation } from "../proto/gen/meta";
import { getActors, getMaterializedViews } from "./api/streaming";
import StreamingView from '../components/StreamingView'
import NoData from '../components/NoData'

export default function Streaming() {
    const toast = useToast();
    const [actorProtoList, setActorProtoList] = useState<ActorLocation[]>([]);
    const [mvList, setMvList] = useState<Table[]>([]);

    useEffect(() => {
        async function doFetch() {
            try {
                setActorProtoList(await getActors());
                setMvList(await getMaterializedViews());
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
            <Title>Streaming</Title>
            <NoData />
        </Box>
    );
}