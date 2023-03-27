package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorRecordSetProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.RecordSet;

import javax.inject.Inject;
import java.util.List;

public class IoTDBRecordSetProvider implements ConnectorRecordSetProvider {

    private final IoTDBClient ioTDBClient;

    @Inject
    public IoTDBRecordSetProvider(IoTDBClient ioTDBClient) {
        this.ioTDBClient = ioTDBClient;
    }

    @Override
    public RecordSet getRecordSet(ConnectorTransactionHandle transaction,
                                  ConnectorSession session,
                                  ConnectorSplit split,
                                  ConnectorTableHandle table,
                                  List<? extends ColumnHandle> columns)
    {
        IoTDBSplit iotdbSplit = (IoTDBSplit) split;

        ImmutableList.Builder<IoTDBColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add((IoTDBColumnHandle) handle);
        }

        return new IoTDBRecordSet(ioTDBClient, iotdbSplit, handles.build());
    }
}
