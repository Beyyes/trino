package io.trino.plugin.iotdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class IoTDBTableHandle implements ConnectorTableHandle {
    private final String schemaName;
    private final String tableName;
    private final Optional<TupleDomain<ColumnHandle>> predicate;

    @JsonCreator
    public IoTDBTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName) {
        this(schemaName, tableName, Optional.empty());
    }

    private IoTDBTableHandle(String schemaName, String tableName, Optional<TupleDomain<ColumnHandle>> predicate) {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.predicate = requireNonNull(predicate, "predicate is null");
    }

    @JsonProperty
    public String getSchemaName() {
        return schemaName;
    }

    @JsonProperty
    public String getTableName() {
        return tableName;
    }

    public Optional<TupleDomain<ColumnHandle>> getPredicate() {
        return this.predicate;
    }

    public SchemaTableName toSchemaTableName() {
        return new SchemaTableName(schemaName, tableName);
    }

    public IoTDBTableHandle withPredicate(TupleDomain<ColumnHandle> predicate) {
        return new IoTDBTableHandle(schemaName, tableName, Optional.of(predicate));
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName, tableName);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        IoTDBTableHandle other = (IoTDBTableHandle) obj;
        return Objects.equals(this.schemaName, other.schemaName) &&
                Objects.equals(this.tableName, other.tableName);
    }

    @Override
    public String toString() {
        return schemaName + ":" + tableName;
    }
}
