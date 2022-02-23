import '../styles/global.css'

import { useState, useEffect } from 'react';
import { useRouter } from 'next/router';
import Layout from "../components/Layout";

// The entry point of the website. It is used to define some global variables.
export default function App({ Component, pageProps }) {

  const router = useRouter();
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    router.events.on("routeChangeStart", () => setIsLoading(true));
    router.events.on('routeChangeComplete', () => setIsLoading(false));
    router.events.on('routeChangeError', () => setIsLoading(false));
  })

  return isLoading
    ? <div>Loading</div> 
    : <Layout>
      <Component {...pageProps} />
    </Layout> ;
}

