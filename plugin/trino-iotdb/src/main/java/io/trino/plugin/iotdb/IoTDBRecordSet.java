package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.apache.iotdb.isession.SessionDataSet;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class IoTDBRecordSet implements RecordSet {

    private final IoTDBTableHandle tableHandle;

    private final List<IoTDBColumnHandle> columnHandles;

    private final List<Type> columnTypes;

    private final SessionDataSet sessionDataSet;

    public IoTDBRecordSet(IoTDBClient client,
                          IoTDBSplit split,
                          IoTDBTableHandle tableHandle,
                          List<IoTDBColumnHandle> columnHandles) {

        requireNonNull(client, "IoTDBClient is null");
        requireNonNull(split, "IoTDBSplit is null");

        this.tableHandle = requireNonNull(tableHandle, "IoTDBTableHandle is null");
        this.columnHandles = requireNonNull(columnHandles, "IoTDBColumnHandles is null");
        this.columnTypes = columnHandles.stream().map(IoTDBColumnHandle::getColumnType).toList();
        this.sessionDataSet = client.query(tableHandle, columnHandles);
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new IoTDBRecordCursor(sessionDataSet, columnHandles);
    }
}
