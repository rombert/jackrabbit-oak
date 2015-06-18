package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.junit.Test;

public class MultiplexingDocumentStoreTest {
    
    @Test
    public void find_noResults() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
            .root(root)
            .mount("/var", var)
            .build();
        
        assertNull(store.find(Collection.NODES, "0:/"));
    }
    
    @Test
    public void find_matching() {

        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        UpdateOp updateOp = new UpdateOp("0:/", true);
        updateOp.set("prop", "val");
        root.createOrUpdate(Collection.NODES, updateOp);
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
            .root(root)
            .mount("/var", var)
            .build();
        
        assertNotNull(store.find(Collection.NODES, "0:/"));
    }

    @Test
    public void find_wrongStore() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        UpdateOp updateOp = new UpdateOp("0:/", true);
        updateOp.set("prop", "val");
        var.createOrUpdate(Collection.NODES, updateOp);
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/var", var)
                .build();
        
        assertNull(store.find(Collection.NODES, "0:/"));
    }

}
