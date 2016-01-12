package org.apache.jackrabbit.oak.fixture;

import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

public class MultiplexedMemoryFixture extends NodeStoreFixture {

    @Override
    public NodeStore createNodeStore() {
        
        MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                .mount("tmp", "/tmp")
                .build();
        
        DocumentMK.Builder builder = new DocumentMK.Builder();
        builder.setMountInfoProvider(mip);
        builder.addMemoryMount("tmp");

        return builder.getNodeStore();
    }
    
    @Override
    public String toString() {
        return getClass().getSimpleName();
    }
    
    @Override
    public void dispose(NodeStore nodeStore) {
        if ( nodeStore instanceof DocumentNodeStore ) {
            ((DocumentNodeStore) nodeStore).dispose();
        }
    }

}
