/*
 * Copyright 2023 RisingWave Labs
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

import { useToast } from "@chakra-ui/react"
import { useEffect, useState } from "react"

export default function useFetch<T>(fetchFn: () => Promise<T>) {
  const [response, setResponse] = useState<T>()
  const toast = useToast()

  useEffect(() => {
    const fetchData = async () => {
      try {
        const res = await fetchFn()
        setResponse(res)
      } catch (e: any) {
        toast({
          title: "Error Occurred",
          description: e.toString(),
          status: "error",
          duration: 5000,
          isClosable: true,
        })
        console.error(e)
      }
    }
    fetchData()
  }, [toast, fetchFn])

  return { response }
}
