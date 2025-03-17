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
import "bootstrap-icons/font/bootstrap-icons.css"
import "reflect-metadata"
import "../styles/global.css"

import { ChakraProvider } from "@chakra-ui/react"
import { loader as monacoLoader } from "@monaco-editor/react"
import type { AppProps } from "next/app"
import { useRouter } from "next/router"
import { useEffect, useState } from "react"
import Layout from "../components/Layout"
import SpinnerOverlay from "../components/SpinnerOverlay"

// The entry point of the website. It is used to define some global variables.
function App({ Component, pageProps }: AppProps) {
  const router = useRouter()
  const [isLoading, setIsLoading] = useState(false)

  useEffect(() => {
    router.events.on("routeChangeStart", (url, { shallow }) => {
      if (!shallow) {
        setIsLoading(true)
      }
    })
    router.events.on("routeChangeComplete", () => setIsLoading(false))
    router.events.on("routeChangeError", () => setIsLoading(false))
  }, [router.events])

  useEffect(() => {
    // Use vendored assets to get rid of fetching from CDN.
    monacoLoader.config({ paths: { vs: "/monaco" } })
  })

  return (
    <ChakraProvider>
      <Layout>
        {isLoading ? <SpinnerOverlay /> : <Component {...pageProps} />}
      </Layout>
    </ChakraProvider>
  )
}

export default App
