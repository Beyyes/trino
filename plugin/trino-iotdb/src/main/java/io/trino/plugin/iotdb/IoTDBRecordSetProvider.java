package io.trino.plugin.iotdb;

import io.trino.spi.connector.ConnectorRecordSetProvider;

import javax.inject.Inject;

public class IoTDBRecordSetProvider implements ConnectorRecordSetProvider {
    @Inject
    public IoTDBRecordSetProvider() {

    }
}
