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

import {
  Box,
  Popover,
  PopoverArrow,
  PopoverContent,
  PopoverTrigger,
  Text,
} from "@chakra-ui/react"
import loadable from "@loadable/component"
import { Fragment } from "react"
import { Handle, Position } from "react-flow-renderer"

const ReactJson = loadable(() => import("react-json-view"))

const contentStyle = { background: "#FFFFFF" }
function NodeType({ data }: { data: any }) {
  if (data === undefined || data.label === undefined) {
    return <Fragment></Fragment>
  }

  return (
    <Box>
      <Popover isLazy placement="top">
        <PopoverTrigger {...{ contentStyle }}>
          <Box zIndex="0">
            <Text fontSize="8px" textAlign="right" color="LightSlateGray">
              {data.stage}
            </Text>
            <Text fontSize="14px" textAlign="center">
              {data.label}{" "}
            </Text>
          </Box>
        </PopoverTrigger>
        <Box zIndex="popover">
          <PopoverContent>
            <PopoverArrow />
            <Box
              sx={{
                "&::-webkit-scrollbar": {
                  width: "16px",
                  borderRadius: "8px",
                  backgroundColor: `rgba(0, 0, 0, 0.05)`,
                },
                "&::-webkit-scrollbar-thumb": {
                  backgroundColor: `rgba(0, 0, 0, 0.05)`,
                },
              }}
              overflowX="auto"
              maxHeight="400px"
            >
              <ReactJson
                src={data.content}
                name={false}
                displayDataTypes={false}
              />
            </Box>
          </PopoverContent>
        </Box>
      </Popover>

      <Handle type="target" position={Position.Left} />
      <Handle type="source" position={Position.Right} />
    </Box>
  )
}
export default NodeType
