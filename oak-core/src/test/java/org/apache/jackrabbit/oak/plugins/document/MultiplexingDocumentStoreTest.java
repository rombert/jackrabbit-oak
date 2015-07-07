package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertNotNull;

import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.junit.Test;
import org.junit.internal.matchers.TypeSafeMatcher;

import com.google.common.collect.Maps;

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
            .mount("/1c", var)
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
            .mount("/1c", var)
            .build();
        
        List<NodeDocument> nodes = store.query(Collection.NODES, 
                DocumentKeyImpl.fromPath("/1a").getValue(), 
                DocumentKeyImpl.fromPath("/1e").getValue(), 
                1);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1b"));
    }

    @Test
    public void query_atRoot() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/1c", var)
                .build();
        
        List<NodeDocument> nodes = store.query(Collection.NODES, 
                DocumentKeyImpl.fromPath("/").getValue(), 
                DocumentKeyImpl.fromPath("/1b").getValue(), 
                1);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1a"));
    }
    
    @Test
    public void remove_matchingInRootStore() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/1c", var)
                .build();
        
        store.remove(Collection.NODES, DocumentKeyImpl.fromPath("/1a").getValue());
        
        assertThat(store.find(Collection.NODES, DocumentKeyImpl.fromPath("/1a").getValue()), nullValue());
    }
    
    @Test
    public void remove_matchingInOtherStore() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/1c", var)
                .build();
        
        store.remove(Collection.NODES, DocumentKeyImpl.fromPath("/1c").getValue());
        
        assertThat(store.find(Collection.NODES, DocumentKeyImpl.fromPath("/1c").getValue()), nullValue());
    }

    @Test
    public void remove_notMatching() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/1c", var)
                .build();
        
        store.remove(Collection.NODES, DocumentKeyImpl.fromPath("/1z").getValue());
    }

    @Test
    public void remove_multipleKeysInMultipleStores() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/1c", var)
                .build();
        
        store.remove(Collection.NODES, Arrays.asList(
                DocumentKeyImpl.fromPath("/1b").getValue(),
                DocumentKeyImpl.fromPath("/1c").getValue(),
                DocumentKeyImpl.fromPath("/1d").getValue()
        ));
        
        assertThat(store.query(Collection.NODES, DocumentKeyImpl.fromPath("/1a").getValue(), DocumentKeyImpl.fromPath("/1e").getValue(), 10).size(), CoreMatchers.equalTo(0));
    }
    
    @Test
    public void remove_withConditions() {

        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");

        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/1c", var)
                .build();
        
        
        UpdateOp.Key prop = new UpdateOp.Key("prop", null);
        UpdateOp.Condition equalsVal = Condition.newEqualsCondition("val");
        UpdateOp.Condition equalsOtherval = Condition.newEqualsCondition("otherVal");

        // /1b with prop = val ( -> REMOVED )
        // /1c with prop = val ( -> REMOVED )
        // /1d with prop = otherval ( -> NOT REMOVED )

        Map<String, Map<Key, Condition>> conditionedRemovals = Maps.newHashMap();
        conditionedRemovals.put(DocumentKeyImpl.fromPath("/1b").getValue(), Collections.singletonMap(prop, equalsVal));
        conditionedRemovals.put(DocumentKeyImpl.fromPath("/1c").getValue(), Collections.singletonMap(prop, equalsVal));
        conditionedRemovals.put(DocumentKeyImpl.fromPath("/1d").getValue(), Collections.singletonMap(prop, equalsOtherval));
        
        assertThat(store.remove(Collection.NODES, conditionedRemovals), equalTo(2));
        
        List<NodeDocument> nodes = store.query(Collection.NODES, 
                DocumentKeyImpl.fromPath("/1a").getValue(), 
                DocumentKeyImpl.fromPath("/1e").getValue(), 
                10);
        
        assertThat(nodes, NodeListMatcher.nodeListWithKeys("1:/1d"));
    }
    
    @Test
    public void create() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");

        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/1c", var)
                .build();
        
        UpdateOp rootOp = new UpdateOp("1:/1f", true);
        rootOp.set(Document.ID, "1:/1f");
        
        UpdateOp subOp = new UpdateOp("2:/1c/a", true);
        subOp.set(Document.ID, "2:/1c/a");
        
        boolean created = store.create(Collection.NODES, Arrays.asList(rootOp, subOp));

        assertTrue(created);
        
        assertNotNull(root.find(Collection.NODES, "1:/1f"));
        assertNotNull(var.find(Collection.NODES, "2:/1c/a"));
    }

    @Test
    public void update() {
        
        DocumentStore root = new MemoryDocumentStore();
        DocumentStore var = new MemoryDocumentStore();
        
        writeNode(root, "/1a");
        writeNode(root, "/1b");
        writeNode(var, "/1c");
        writeNode(root, "/1d");
        writeNode(root, "/1e");
        
        MultiplexingDocumentStore store = new MultiplexingDocumentStore.Builder()
                .root(root)
                .mount("/1c", var)
                .build();
        
        
        UpdateOp update = new UpdateOp("", false);
        update.set("prop", "newVal");
        
        store.update(Collection.NODES, Arrays.asList("1:/1a", "1:/1c"), update);
        
        assertThat(store.find(Collection.NODES, "1:/1a").get("prop"), equalTo((Object) "newVal"));
        assertThat(store.find(Collection.NODES, "1:/1c").get("prop"), equalTo((Object) "newVal"));
        
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
