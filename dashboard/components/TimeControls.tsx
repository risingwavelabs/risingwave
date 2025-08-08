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
  FormControl,
  FormLabel,
  HStack,
  Input,
  Text,
  VStack,
} from "@chakra-ui/react"
import { useState } from "react"
import {
  getCurrentTimeInSystemTimezone,
  parseDuration,
  parseTimestampToUnixEpoch,
} from "../lib/utils/timeUtils"

interface TimeControlsProps {
  onApply: (timestamp?: number, offset?: number) => void
  title?: string
}

export default function TimeControls({
  onApply,
  title = "Time Controls",
}: TimeControlsProps) {
  const [timestampInput, setTimestampInput] = useState("")
  const [offsetInput, setOffsetInput] = useState("")
  const [displayTimestamp, setDisplayTimestamp] = useState<string>("")
  const [displayOffset, setDisplayOffset] = useState<string>("")
  const [timestampError, setTimestampError] = useState<string>("")
  const [offsetError, setOffsetError] = useState<string>("")

  const handleApply = () => {
    let timestamp: number | undefined
    let offset: number | undefined

    // Parse timestamp if provided
    if (timestampInput.trim()) {
      try {
        timestamp = parseTimestampToUnixEpoch(timestampInput)
        setDisplayTimestamp(new Date(timestamp * 1000).toISOString())
        setTimestampError("")
      } catch (error) {
        setTimestampError("Invalid timestamp format")
        return
      }
    } else {
      setDisplayTimestamp("")
    }

    // Parse offset if provided
    if (offsetInput.trim()) {
      try {
        offset = parseDuration(offsetInput)
        setDisplayOffset(offsetInput)
        setOffsetError("")
      } catch (error) {
        setOffsetError("Invalid offset format (use: 30s, 5m, 2h, 1d)")
        return
      }
    } else {
      setDisplayOffset("")
    }

    onApply(timestamp, offset)
  }

  const handleReset = () => {
    setTimestampInput("")
    setOffsetInput("")
    setDisplayTimestamp("")
    setDisplayOffset("")
    setTimestampError("")
    setOffsetError("")
    onApply(undefined, undefined)
  }

  return (
    <Box>
      <Text fontWeight="semibold" mb={3}>
        {title}
      </Text>
      <VStack spacing={3} align="stretch">
        <FormControl isInvalid={!!timestampError}>
          <FormLabel fontSize="sm">Timestamp (system timezone)</FormLabel>
          <HStack spacing={2}>
            <Input
              size="sm"
              placeholder="2024-01-15T10:30:00 (system timezone)"
              value={timestampInput}
              onChange={(e) => setTimestampInput(e.target.value)}
            />
            <Button
              size="sm"
              variant="outline"
              onClick={() => {
                const now = getCurrentTimeInSystemTimezone()
                setTimestampInput(now)
                setTimestampError("")
              }}
            >
              Now
            </Button>
          </HStack>
          {timestampError && (
            <Text fontSize="xs" color="red.500">
              {timestampError}
            </Text>
          )}
        </FormControl>

        <FormControl isInvalid={!!offsetError}>
          <FormLabel fontSize="sm">Offset</FormLabel>
          <Input
            size="sm"
            placeholder="1h, 30m, 2d"
            value={offsetInput}
            onChange={(e) => setOffsetInput(e.target.value)}
          />
          {offsetError && (
            <Text fontSize="xs" color="red.500">
              {offsetError}
            </Text>
          )}
        </FormControl>

        <HStack spacing={2}>
          <Button size="sm" colorScheme="blue" onClick={handleApply}>
            Apply
          </Button>
          <Button size="sm" variant="outline" onClick={handleReset}>
            Reset
          </Button>
        </HStack>

        {(displayTimestamp || displayOffset) && (
          <Box p={2} bg="gray.50" borderRadius="md">
            <Text fontSize="xs" fontWeight="medium" mb={1}>
              Applied Settings:
            </Text>
            {displayTimestamp && (
              <Text fontSize="xs" color="gray.600">
                Timestamp: {displayTimestamp}
              </Text>
            )}
            {displayOffset && (
              <Text fontSize="xs" color="gray.600">
                Offset: {displayOffset}
              </Text>
            )}
          </Box>
        )}
      </VStack>
    </Box>
  )
}
