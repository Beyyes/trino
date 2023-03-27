package io.trino.plugin.iotdb;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
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
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.common.type.FloatType;
import org.apache.iotdb.tsfile.read.common.type.IntType;

public class IoTDBRecordCursor implements RecordCursor {

    private final SessionDataSet sessionDataSet;

    private RowRecord rowRecord;

    public IoTDBRecordCursor(SessionDataSet sessionDataSet) {
        this.sessionDataSet = sessionDataSet;
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
        TSDataType dataType = rowRecord.getFields().get(field).getDataType();
        return Utils.transferIoTDBType(dataType);
    }

    @Override
    public boolean advanceNextPosition() {
        try {
            if (!sessionDataSet.hasNext()) {
                return false;
            }

            rowRecord = sessionDataSet.next();

        } catch (StatementExecutionException | IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public boolean getBoolean(int field) {
        return rowRecord.getFields().get(field).getBoolV();
    }

    @Override
    public long getLong(int field) {
        return rowRecord.getFields().get(field).getLongV();
    }

    @Override
    public double getDouble(int field) {
        return rowRecord.getFields().get(field).getDoubleV();
    }

    @Override
    public Slice getSlice(int field) {
        return Slices.utf8Slice(rowRecord.getFields().get(field).getStringValue());
    }

    @Override
    public Object getObject(int field) {
        return rowRecord.getFields().get(field).getStringValue();
    }

    @Override
    public boolean isNull(int field) {
        return rowRecord.getFields().get(field).getDataType() == null;
    }

    @Override
    public void close() {
    }
}
