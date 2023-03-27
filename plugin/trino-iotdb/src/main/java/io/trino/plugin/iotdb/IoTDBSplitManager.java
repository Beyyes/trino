package io.trino.plugin.iotdb;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class IoTDBSplitManager implements ConnectorSplitManager {

    private final IoTDBClient ioTDBClient;

    @Inject
    public IoTDBSplitManager(IoTDBClient ioTDBClient) {
        this.ioTDBClient = requireNonNull(ioTDBClient, "iotdbClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle connectorTableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint) {
        return null;
    }
}
