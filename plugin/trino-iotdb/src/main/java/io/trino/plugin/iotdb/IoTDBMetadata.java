package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class IoTDBMetadata implements ConnectorMetadata {

    private final IoTDBClient iotdbClient;

    @Inject
    public IoTDBMetadata(IoTDBClient iotdbClient) {
        this.iotdbClient = requireNonNull(iotdbClient, "IoTDBClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return List.of("root");
    }

    public List<String> listSchemaNames() {
        return List.of("root");
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        return getTableMetadata(((IoTDBTableHandle) table).toSchemaTableName());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName) {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(ImmutableSet.of("root")));

        return schemaNames.stream()
                .flatMap(schemaName -> iotdbClient.getTableNamesFromGivenSchema(schemaName).stream().
                        map(tableName -> new SchemaTableName(schemaName, tableName)))
                .collect(toImmutableList());
    }

    @Override
    public IoTDBTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName) {
        if (!listSchemaNames(session).contains(tableName.getSchemaName())) {
            return null;
        }

//        if (iotdbClient.getTable(tableName.getSchemaName(), tableName.getTableName()) == null) {
//            return null;
//        }

        return new IoTDBTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle) {
        IoTDBTableHandle iotdbTableHandle = (IoTDBTableHandle) tableHandle;

        List<ColumnMetadata> columnMetadataList = iotdbClient.getColumnMetadataFromTable(iotdbTableHandle.getSchemaName(),
                iotdbTableHandle.getTableName());
        if (columnMetadataList == null) {
            throw new TableNotFoundException(iotdbTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();
        int index = 0;
        for (ColumnMetadata column : columnMetadataList) {
            columnHandles.put(column.getName(), new IoTDBColumnHandle(column.getName(), column.getType(), index));
            index++;
        }
        return columnHandles.buildOrThrow();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName) {
        if (!listSchemaNames().contains(schemaTableName.getSchemaName())) {
            return null;
        }

        List<ColumnMetadata> columns = iotdbClient.getColumnMetadataFromTable(schemaTableName.getSchemaName(),
                schemaTableName.getTableName());
        return new ConnectorTableMetadata(schemaTableName, columns);
    }
}
