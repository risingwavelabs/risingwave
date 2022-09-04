import { Flex, Spinner } from "@chakra-ui/react"

function SpinnerOverlay() {
    return <Flex w="calc(100vw - 320px)"
        alignItems="center"
        justifyContent="center"
        position="fixed"
        top={0}
        bottom={0}>
        <Spinner
            thickness='4px'
            speed='0.65s'
            emptyColor='gray.200'
            color='blue.500'
            size='xl'
        />
    </Flex>
}

export default SpinnerOverlay