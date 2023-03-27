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
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class IoTDBMetadata implements ConnectorMetadata {

    public static final String IOTDB_DEFAULT_SCHEMA = "root";

    private final IoTDBClient iotdbClient;

    @Inject
    public IoTDBMetadata(IoTDBClient iotdbClient) {
        this.iotdbClient = requireNonNull(iotdbClient, "IoTDBClient is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session) {
        return List.of(IOTDB_DEFAULT_SCHEMA);
    }

    public List<String> listSchemaNames() {
        return List.of(IOTDB_DEFAULT_SCHEMA);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table) {
        SchemaTableName schemaTableName = ((IoTDBTableHandle) table).toSchemaTableName();

        if (!listSchemaNames().contains(schemaTableName.getSchemaName())) {
            return null;
        }

        List<ColumnMetadata> columns = iotdbClient.getColumnMetadataFromTable(schemaTableName.getSchemaName(),
                schemaTableName.getTableName());
        return new ConnectorTableMetadata(schemaTableName, columns);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName) {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(ImmutableSet.of(IOTDB_DEFAULT_SCHEMA)));

        return schemaNames.stream()
                .flatMap(schemaName -> iotdbClient.getTableNamesFromGivenSchema(schemaName).stream().
                        map(tableName -> new SchemaTableName(schemaName, tableName)))
                .collect(toImmutableList());
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix) {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName schemaTable : listTables(session, Optional.empty())) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(schemaTable);
            if (tableMetadata != null) {
                columns.put(schemaTable, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
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

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session,
                                            ConnectorTableHandle tableHandle,
                                            ColumnHandle columnHandle) {
        return ((IoTDBColumnHandle) columnHandle).getColumnMetadata();
    }

    public ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTable) {
        if (!listSchemaNames().contains(schemaTable.getSchemaName())) {
            return null;
        }

        List<ColumnMetadata> columns = iotdbClient.getColumnMetadataFromTable(schemaTable.getSchemaName(),
                schemaTable.getTableName());
        return new ConnectorTableMetadata(schemaTable, columns);
    }

    private List<ColumnMetadata> getColumnMetadataFromTable(SchemaTableName schemaTable) {
        List<ColumnMetadata> columns = iotdbClient.getColumnMetadataFromTable(schemaTable.getSchemaName(),
                schemaTable.getTableName());
        return columns;

    }

}
