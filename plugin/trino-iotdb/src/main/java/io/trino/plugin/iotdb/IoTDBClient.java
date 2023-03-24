package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.TrinoException;
import org.apache.iotdb.isession.util.Version;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import javax.inject.Inject;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import static java.util.Objects.requireNonNull;

public class IoTDBClient {

    private static String host = "127.0.0.1";
    private static int port = 6667;
    private static Session session;

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

    public Set<String> getTableNames(String schema) {
        requireNonNull(schema, "schema is null");
        Set<String> tableNames = new HashSet<>();
        try (SessionDataSet dataSet = session.executeQueryStatement("show devices")) {
            System.out.println(dataSet.getColumnNames());
            while (dataSet.hasNext()) {
                RowRecord r = dataSet.next();
                tableNames.add(String.valueOf(r.getFields().get(0)));
            }
        } catch (IoTDBConnectionException e) {
            throw new RuntimeException(e);
        } catch (StatementExecutionException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    public static void main(String[] args) {
        new IoTDBClient().getTableNames("root");
    }
}
