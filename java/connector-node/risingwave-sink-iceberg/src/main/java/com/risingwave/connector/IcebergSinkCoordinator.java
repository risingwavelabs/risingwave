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
 */

package com.risingwave.connector;

import com.risingwave.connector.api.sink.SinkCoordinator;
import com.risingwave.java.utils.ObjectSerde;
import com.risingwave.proto.ConnectorServiceProto;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;

public class IcebergSinkCoordinator implements SinkCoordinator {

    private final Table icebergTable;

    public IcebergSinkCoordinator(Table icebergTable) {
        this.icebergTable = icebergTable;
    }

    @Override
    public void commit(long epoch, List<ConnectorServiceProto.SinkMetadata> metadataList) {
        List<DataFile> dataFileList = new ArrayList<>(metadataList.size());
        List<DeleteFile> deleteFileList = new ArrayList<>(metadataList.size());
        for (ConnectorServiceProto.SinkMetadata metadata : metadataList) {
            IcebergMetadata icebergMetadata =
                    (IcebergMetadata)
                            ObjectSerde.deserializeObject(
                                    metadata.getSerialized().getMetadata().toByteArray());
            dataFileList.addAll(
                    Arrays.stream(icebergMetadata.dataFiles).collect(Collectors.toList()));
            deleteFileList.addAll(
                    Arrays.stream(icebergMetadata.deleteFiles).collect(Collectors.toList()));
        }
        boolean nonEmpty = false;
        Transaction txn = icebergTable.newTransaction();
        if (!deleteFileList.isEmpty()) {
            RowDelta rowDelta = txn.newRowDelta();
            for (DeleteFile deleteFile : deleteFileList) {
                rowDelta.addDeletes(deleteFile);
            }
            rowDelta.commit();
            nonEmpty = true;
        }

        if (!dataFileList.isEmpty()) {
            AppendFiles append = txn.newAppend();
            for (DataFile dataFile : dataFileList) {
                append.appendFile(dataFile);
            }
            append.commit();
            nonEmpty = true;
        }

        if (nonEmpty) {
            txn.commitTransaction();
        }
    }

    @Override
    public void drop() {}
}
