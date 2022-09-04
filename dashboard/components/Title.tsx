import { Text } from '@chakra-ui/react';
import React from 'react';

function Title({ children }: { children: React.ReactNode }) {
    return <Text mb={2} textColor="blue.500" fontWeight="semibold" lineHeight="6">{children}</Text>
}
export default Title