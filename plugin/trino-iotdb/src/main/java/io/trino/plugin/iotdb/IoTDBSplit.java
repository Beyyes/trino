package io.trino.plugin.iotdb;

import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

public class IoTDBSplit implements ConnectorSplit {

    private final List<HostAddress> addresses;

    private final String hostAddress;

    public IoTDBSplit(String hostAddress) {
        this.hostAddress = hostAddress;
        hostAddress = "127.0.0.1";
        addresses = ImmutableList.of(HostAddress.fromParts("127.0.0.1", 6667));
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public Object getInfo() {
        return this;
    }
}
