package io.trino.plugin.iotdb;

import io.trino.spi.connector.ConnectorSplitManager;

import javax.inject.Inject;

public class IoTDBSplitManager implements ConnectorSplitManager {
    @Inject
    public IoTDBSplitManager() {

    }
}
