package com.risingwave.connector.api;

import com.risingwave.proto.Data;

public class ColumnDesc {
    String name;
    Data.DataType dataType;

    public ColumnDesc(String name, Data.DataType dataType) {
        this.name = name;
        this.dataType = dataType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Data.DataType getDataType() {
        return dataType;
    }

    public void setDataType(Data.DataType dataType) {
        this.dataType = dataType;
    }
}
