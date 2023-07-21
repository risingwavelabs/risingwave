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
        if (!dataFileList.isEmpty()) {
            AppendFiles append = txn.newAppend();
            for (DataFile dataFile : dataFileList) {
                append.appendFile(dataFile);
            }
            append.commit();
            nonEmpty = true;
        }

        if (!deleteFileList.isEmpty()) {
            RowDelta rowDelta = txn.newRowDelta();
            for (DeleteFile deleteFile : deleteFileList) {
                rowDelta.addDeletes(deleteFile);
            }
            rowDelta.commit();
            nonEmpty = true;
        }

        if (nonEmpty) {
            txn.commitTransaction();
        }
    }

    @Override
    public void drop() {}
}
