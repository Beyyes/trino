package io.trino.plugin.iotdb;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class IoTDBSplit implements ConnectorSplit {

    private static final int INSTANCE_SIZE = instanceSize(IoTDBSplit.class);

    private final List<HostAddress> addresses;

    @JsonCreator
    public IoTDBSplit(@JsonProperty("addresses") List<HostAddress> addresses) {
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @Override
    public boolean isRemotelyAccessible() {
        return true;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses() {
        return addresses;
    }

    @Override
    public Object getInfo() {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes() {
        return INSTANCE_SIZE + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }

}
