package org.apache.jackrabbit.oak.fixture;

import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.amazonaws.services.redshift.model.UnauthorizedOperationException;

public class MemoryMultiplexedFixture extends NodeStoreFixture {

    @Override
    public NodeStore createNodeStore() {
        throw new UnauthorizedOperationException("Not implemented");
    }
    
    @Override
    public String toString() {
        return "Multiplexed Memory NodeStore";
    }

}
