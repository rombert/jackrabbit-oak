package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertNotNull;

import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;

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
    
    @Test
    public void query_onSingleStore() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(root, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
            .root(root)
            .mount("/var", var)
            .build();
        
        List<NodeDocument> nodes = store.query(Collection.NODES, 
                DocumentKeyImpl.fromPath("/1a").getValue(), 
                DocumentKeyImpl.fromPath("/1e").getValue(), 
                10);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1b", "1:/1c", "1:/1d"));
    }
    
    @Test
    public void query_multipleStores() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
            .root(root)
            .mount("/1f", var)
            .build();
        
        List<NodeDocument> nodes = store.query(Collection.NODES, 
                DocumentKeyImpl.fromPath("/1a").getValue(), 
                DocumentKeyImpl.fromPath("/1e").getValue(), 
                10);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1b", "1:/1c", "1:/1d"));
    }
    
    @Test
    public void query_multipleStores_obeysLimit() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
            .root(root)
            .mount("/1f", var)
            .build();
        
        List<NodeDocument> nodes = store.query(Collection.NODES, 
                DocumentKeyImpl.fromPath("/1a").getValue(), 
                DocumentKeyImpl.fromPath("/1e").getValue(), 
                1);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1b"));
    }       

    private void writeNode(DocumentStore root, String path) {
        String id = DocumentKeyImpl.fromPath(path).getValue();
        UpdateOp updateOp = new UpdateOp(id, true);
        updateOp.set(Document.ID, id);
        updateOp.set("prop", "val");
        root.createOrUpdate(Collection.NODES, updateOp);
    }

    static class NodeListMatcher extends TypeSafeMatcher<List<NodeDocument>> {
        
        public static NodeListMatcher nodeListWithKeys(String first, String... others) {
            
            List<String> keys = new ArrayList<String>();
            keys.add(first);
            for ( String other: others) {
                keys.add(other);
            }
            
            return new NodeListMatcher(keys);
        }

        private final List<String> keys;
        
        private NodeListMatcher(List<String> keys) {
            this.keys = keys;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("A list of " + keys.size() + " node documents with the following keys: " + keys);
            
        }

        @Override
        public boolean matchesSafely(List<NodeDocument> toMatch) {
            if ( toMatch.size() != keys.size() ) {
                return false;
            }

            for ( int i = 0; i < toMatch.size() ; i++ ) {
                if ( !keys.get(i).equals(toMatch.get(i).getId())) {
                    return false;
                }
            }
            
            return true;
        }
        
    }
}
