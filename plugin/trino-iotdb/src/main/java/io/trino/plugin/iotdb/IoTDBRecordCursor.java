package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.type.FloatType;
import org.apache.iotdb.tsfile.read.common.type.IntType;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.iotdb.IoTDBClient.TIME_COLUMN;

public class IoTDBRecordCursor implements RecordCursor {

    private final SessionDataSet sessionDataSet;

    private RowRecord rowRecord;

    private final List<IoTDBColumnHandle> columnHandles;

    private List<Field> fields;

    private final int[] fieldToColumnIndex;

    public IoTDBRecordCursor(SessionDataSet sessionDataSet, List<IoTDBColumnHandle> columnHandles) {
        this.sessionDataSet = sessionDataSet;
        this.columnHandles = columnHandles;
        this.fieldToColumnIndex = new int[columnHandles.size()];
        this.fields = new ArrayList<>();

        // IoTDB sessionDataSet format [Time, root.db.machine.address, root.db.machine.host, root.db.machine.cpu]
        // Trino row format [Time, address, host, cpu]
        List<String> ioTDBColumns = new ArrayList<>();
        for (String iotdbColumnName : sessionDataSet.getColumnNames()) {
            if (TIME_COLUMN.equalsIgnoreCase(iotdbColumnName)) {
                ioTDBColumns.add(TIME_COLUMN.toLowerCase());
            } else {
                String[] splits = iotdbColumnName.split("\\.");
                ioTDBColumns.add(splits[splits.length - 1].toLowerCase());
            }
        }

        // columnHandles[0] is always Time column
        for (int i = 0; i < columnHandles.size(); i++) {
            IoTDBColumnHandle handle = columnHandles.get(i);
            int index = ioTDBColumns.indexOf(handle.getColumnName().toLowerCase());
            if (index == -1) {
                throw new RuntimeException("Cannot find column " + handle.getColumnName().toLowerCase());
            }
            fieldToColumnIndex[i] = index;
        }
    }

    @Override
    public long getCompletedBytes() {
        return 0;
    }

    @Override
    public long getReadTimeNanos() {
        return 0;
    }

    @Override
    public Type getType(int field) {
        TSDataType dataType = fields.get(field).getDataType();
        return Utils.transferIoTDBType(dataType);
    }

    @Override
    public boolean advanceNextPosition() {
        try {
            if (!sessionDataSet.hasNext()) {
                return false;
            }

            rowRecord = sessionDataSet.next();
            fields.clear();

            for (int i = 0; i < columnHandles.size(); i++) {
                IoTDBColumnHandle handle = columnHandles.get(i);
                if (handle.getColumnName().equalsIgnoreCase(TIME_COLUMN)) {
                    fields.add(Field.getField(rowRecord.getTimestamp(), TSDataType.INT64));
                } else {
                    Field targetField = rowRecord.getFields().get(fieldToColumnIndex[i] - 1);
                    fields.add(targetField);
                }
            }
        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        return fields.get(field).getBoolV();
    }

    @Override
    public long getLong(int field) {
        return fields.get(field).getLongV();
    }

    @Override
    public double getDouble(int field) {
        return fields.get(field).getDoubleV();
    }

    @Override
    public Slice getSlice(int field) {
        return Slices.utf8Slice(fields.get(field).getStringValue());
    }

    @Override
    public Object getObject(int field) {
        return fields.get(field).getStringValue();
    }

    @Override
    public boolean isNull(int field) {
        return fields.get(field).getDataType() == null;
    }

    @Override
    public void close() {
    }
}
