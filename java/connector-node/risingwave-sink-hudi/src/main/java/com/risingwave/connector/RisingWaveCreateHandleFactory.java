package com.risingwave.connector;

import org.apache.hudi.io.CreateHandleFactory;

public class RisingWaveCreateHandleFactory extends CreateHandleFactory {
    protected String getNextFileId(String idPfx) {
        return HoodieRisingWaveWriter.RISINGWAVE_FILE_ID;
    }
}
