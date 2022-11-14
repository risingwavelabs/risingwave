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
import { useEffect, useState } from "react"

export default function useWindowSize() {
  const [size, setSize] = useState({ w: 1024, h: 768 })
  // setSize({
  //   w: window.innerWidth,
  //   h: window.innerHeight
  // })
  const setSizeOnEvent = () => {
    setSize({
      w: window.innerWidth,
      h: window.innerHeight,
    })
  }
  useEffect(() => {
    window.addEventListener("resize", setSizeOnEvent)
    return () => window.removeEventListener("resize", setSizeOnEvent)
  }, [])
  return size
}
