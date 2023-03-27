/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableList;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.DoubleType;
import org.apache.iotdb.isession.SessionDataSet;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class IoTDBClient {

    private static String host = "127.0.0.1";
    private static int port = 6667;
    private Session session;

    @Inject
    public IoTDBClient() {
        session =
                new Session.Builder()
                        .host(host)
                        .port(port)
                        .username("root")
                        .password("root")
                        .build();
        try {
            session.open(false);
        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        }
        // set session fetchSize
        session.setFetchSize(10000);
    }

    public Set<String> getTableNamesFromGivenSchema(String schema) {
        requireNonNull(schema, "schema is null");
        Set<String> tableNames = new HashSet<>();
        try (SessionDataSet dataSet = session.executeQueryStatement("show devices")) {
            while (dataSet.hasNext()) {
                RowRecord r = dataSet.next();
                tableNames.add(String.valueOf(r.getFields().get(0)).substring(5));
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        return tableNames;
    }

    public SessionDataSet query() {
        try (SessionDataSet dataSet = session.executeQueryStatement("select * from root.**")) {
            return dataSet;
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    public List<ColumnMetadata> getColumnMetadataFromTable(String schema, String tableName) {
        List<ColumnMetadata> columnMetadataList = new ArrayList<>();
        String timeSeries = schema + "." + tableName + ".**";
        try (SessionDataSet dataSet = session.executeQueryStatement("show timeseries " + timeSeries)) {
            while (dataSet.hasNext()) {
                RowRecord r = dataSet.next();
                String timeSeriesName = r.getFields().get(0).getStringValue();
                String[] splits = timeSeriesName.split("\\.");
                String columnName = splits[splits.length - 1];
                ColumnMetadata c = ColumnMetadata.builder()
                        .setName(columnName)
                        .setType(Utils.transferIoTDBType(r.getFields().get(3).getStringValue()))
                        .setNullable(false)
                        .build();
                columnMetadataList.add(c);
            }
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        return columnMetadataList;
    }

    public static void main(String[] args) {
        new IoTDBClient().getTableNamesFromGivenSchema("root");
    }
}
