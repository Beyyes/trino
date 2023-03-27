package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import org.apache.iotdb.isession.SessionDataSet;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class IoTDBRecordSet implements RecordSet {

    private final List<IoTDBColumnHandle> columnHandles;

    private final List<Type> columnTypes;

    private final SessionDataSet sessionDataSet;

    public IoTDBRecordSet(IoTDBClient iotdbClient,
                          IoTDBSplit split,
                          List<IoTDBColumnHandle> columnHandles)
    {
        requireNonNull(iotdbClient, "iotdbClient is null");
        requireNonNull(split, "split is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (IoTDBColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();

        sessionDataSet = iotdbClient.query();
    }

    @Override
    public List<Type> getColumnTypes() {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor() {
        return new IoTDBRecordCursor(sessionDataSet);
    }
}
