package io.trino.plugin.iotdb;

import io.trino.spi.type.BigintType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RealType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

public class Utils {
    public static Type transferIoTDBType(TSDataType dataType) {
        switch (dataType) {
            case DOUBLE -> {
                return DoubleType.DOUBLE;
            }
            case FLOAT -> {
                return RealType.REAL;
            }
            case INT32 -> {
                return IntegerType.INTEGER;
            }
            case INT64 -> {
                return BigintType.BIGINT;
            }
            case TEXT -> {
                return VarcharType.VARCHAR;
            }
            default -> {
                return null;
            }
        }
    }

    public static Type transferIoTDBType(String dataType) {
        switch (dataType) {
            case "DOUBLE" -> {
                return DoubleType.DOUBLE;
            }
            case "FLOAT" -> {
                return RealType.REAL;
            }
            case "INT32" -> {
                return IntegerType.INTEGER;
            }
            case "INT64" -> {
                return BigintType.BIGINT;
            }
            case "TEXT" -> {
                return VarcharType.VARCHAR;
            }
            default -> {
                return null;
            }
        }
    }
}
