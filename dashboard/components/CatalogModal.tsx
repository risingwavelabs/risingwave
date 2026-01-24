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
  Button,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
} from "@chakra-ui/react"

import Link from "next/link"
import { parseAsInteger, useQueryState } from "nuqs"
import {
  Relation,
  relationIsStreamingJob,
  relationTypeTitleCase,
} from "../lib/api/streaming"
import { ReactJson } from "./Relations"

export function useCatalogModal(relationList: Relation[] | undefined) {
  const [modalId, setModalId] = useQueryState("modalId", parseAsInteger)
  const modalData = relationList?.find((r) => r.id === modalId)

  return [modalData, setModalId] as const
}

export function CatalogModal({
  modalData,
  onClose,
}: {
  modalData: Relation | undefined
  onClose: () => void
}) {
  return (
    <Modal isOpen={modalData !== undefined} onClose={onClose} size="3xl">
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>
          Catalog of {modalData && relationTypeTitleCase(modalData)}{" "}
          {modalData?.id} - {modalData?.name}
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          {modalData && (
            <ReactJson
              src={modalData}
              collapsed={1}
              name={null}
              displayDataTypes={false}
            />
          )}
        </ModalBody>

        <ModalFooter>
          {modalData && relationIsStreamingJob(modalData) && (
            <Button colorScheme="blue" mr={3}>
              <Link href={`/fragment_graph/?id=${modalData.id}`}>
                View Fragments
              </Link>
            </Button>
          )}
          <Button mr={3} onClick={onClose}>
            Close
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  )
}
