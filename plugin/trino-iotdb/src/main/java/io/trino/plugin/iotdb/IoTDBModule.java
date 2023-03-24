package io.trino.plugin.iotdb;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;

import static io.airlift.configuration.ConfigBinder.configBinder;

public class IoTDBModule implements Module {

    @Override
    public void configure(Binder binder) {
        binder.bind(IoTDBConnector.class).in(Scopes.SINGLETON);
        binder.bind(IoTDBMetadata.class).in(Scopes.SINGLETON);
        binder.bind(IoTDBClient.class).in(Scopes.SINGLETON);
        binder.bind(IoTDBSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(IoTDBRecordSetProvider.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(IoTDBConnectorConfig.class);
    }
}
