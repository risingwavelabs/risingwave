package com.risingwave.connector;

import java.io.Serializable;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;

public class IcebergMetadata implements Serializable {
    final DataFile[] dataFiles;
    final DeleteFile[] deleteFiles;

    public IcebergMetadata(DataFile[] dataFiles, DeleteFile[] deleteFiles) {
        this.dataFiles = dataFiles;
        this.deleteFiles = deleteFiles;
    }
}
