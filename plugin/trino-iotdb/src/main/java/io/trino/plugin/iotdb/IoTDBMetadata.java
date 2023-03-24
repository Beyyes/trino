package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
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

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName) {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(ImmutableSet.of("root")));

        return schemaNames.stream()
                .flatMap(schemaName ->
                        iotdbClient.getTableNames(schemaName).stream().
                                map(tableName -> new SchemaTableName(schemaName, tableName)))
                .collect(toImmutableList());
    }
}
