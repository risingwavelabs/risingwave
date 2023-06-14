package com.risingwave.connector;

final class IndexGeneratorFactory {

    private IndexGeneratorFactory() {}

    // only support static index currently
    public static IndexGenerator createIndexGenerator(String index
            // List<String> fieldNames,
            // List<DataType> dataTypes,
            // ZoneId localTimeZoneId
            ) {
        return new StaticIndexGenerator(index);
        /*
        final IndexHelper indexHelper = new IndexHelper();
        if (indexHelper.checkIsDynamicIndex(index)) {
            return createRuntimeIndexGenerator(
                    index,
                    fieldNames.toArray(new String[0]),
                    dataTypes.toArray(new DataType[0]),
                    indexHelper,
                    localTimeZoneId);
        } else {
            return new StaticIndexGenerator(index);
        }
        */
    }
}
