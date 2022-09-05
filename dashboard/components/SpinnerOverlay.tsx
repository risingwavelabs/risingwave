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

import { Flex, Spinner } from "@chakra-ui/react"
import { NAVBAR_WIDTH } from "./Layout"

function SpinnerOverlay() {
  return (
    <Flex
      w={`calc(100vw - ${NAVBAR_WIDTH})`}
      alignItems="center"
      justifyContent="center"
      position="fixed"
      top={0}
      bottom={0}
    >
      <Spinner
        thickness="4px"
        speed="0.65s"
        emptyColor="gray.200"
        color="blue.500"
        size="xl"
      />
    </Flex>
  )
}

export default SpinnerOverlay
