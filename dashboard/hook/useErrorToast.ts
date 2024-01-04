import { AlertStatus, useToast } from "@chakra-ui/react"
import { useCallback } from "react"

export default function useErrorToast() {
  const toast = useToast()

  return useCallback(
    (e: any, status: AlertStatus = "error") => {
      let title: string
      let description: string | undefined

      if (e instanceof Error) {
        title = e.message
        description = e.cause?.toString()
      } else {
        title = e.toString()
      }

      toast({
        title,
        description,
        status,
        duration: 5000,
        isClosable: true,
      })

      console.error(e)
    },
    [toast]
  )
}
