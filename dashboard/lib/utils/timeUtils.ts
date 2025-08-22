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

/**
 * Parse a duration string (e.g., "1h", "30m", "2d") and return the duration in seconds
 */
export function parseDuration(durationStr: string): number {
  const match = durationStr.match(/^(\d+)([smhd])$/)
  if (!match) {
    throw new Error(
      'Invalid duration format. Use format like "30s", "5m", "2h", "1d"'
    )
  }

  const value = parseInt(match[1])
  const unit = match[2]

  switch (unit) {
    case "s":
      return value
    case "m":
      return value * 60
    case "h":
      return value * 60 * 60
    case "d":
      return value * 60 * 60 * 24
    default:
      throw new Error("Invalid time unit. Use s, m, h, or d")
  }
}

/**
 * Convert a timestamp and timezone to unix epoch time in seconds
 */
export function parseTimestampToUnixEpoch(
  timestamp: string,
  timezone: string = Intl.DateTimeFormat().resolvedOptions().timeZone
): number {
  try {
    // Create a date object from the timestamp string
    const date = new Date(timestamp)

    if (isNaN(date.getTime())) {
      throw new Error("Invalid timestamp format")
    }

    // Convert to unix epoch time (seconds)
    return Math.floor(date.getTime() / 1000)
  } catch (error) {
    throw new Error("Invalid timestamp format")
  }
}

/**
 * Get current time in system timezone as ISO string
 */
export function getCurrentTimeInSystemTimezone(): string {
  const now = new Date()
  const timeZone = Intl.DateTimeFormat().resolvedOptions().timeZone

  // Format the date in the system timezone
  const formatter = new Intl.DateTimeFormat("en-CA", {
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    timeZone: timeZone,
    hour12: false,
  })

  const formatted = formatter.formatToParts(now)
  const datePart =
    formatted.find((part) => part.type === "year")?.value +
    "-" +
    formatted.find((part) => part.type === "month")?.value +
    "-" +
    formatted.find((part) => part.type === "day")?.value
  const timePart =
    formatted.find((part) => part.type === "hour")?.value +
    ":" +
    formatted.find((part) => part.type === "minute")?.value +
    ":" +
    formatted.find((part) => part.type === "second")?.value

  return `${datePart}T${timePart}`
}

/**
 * Format a unix epoch time to a readable string
 */
export function formatUnixEpoch(unixEpoch: number): string {
  return new Date(unixEpoch * 1000).toISOString()
}
