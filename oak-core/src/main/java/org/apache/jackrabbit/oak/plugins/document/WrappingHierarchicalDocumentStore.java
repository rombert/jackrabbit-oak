package org.apache.jackrabbit.oak.plugins.document;

import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;

public class WrappingHierarchicalDocumentStore implements HierarchicalDocumentStore {
    
    private final DocumentStore store;

    public WrappingHierarchicalDocumentStore(DocumentStore store) {
        this.store = store;
    }

    @Override
    public <T extends Document> T findNode(DocumentKey key) {
        
        return (T) find(Collection.NODES, key.getPath());
    }
    
    @Override
    public <T extends Document> T findNode(DocumentKey key, int maxCacheAge) {
        
        return (T) find(Collection.NODES, key.getPath(), maxCacheAge);
    }
    
    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        
        return find(collection, key, Integer.MAX_VALUE);
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {

        return find0(collection, key, maxCacheAge);
    }
    
    private <T extends Document> T find0(Collection<T> collection, String key, int maxCacheAge) {
        
        if ( collection == Collection.NODES) {
            // caching logic here
            // maybe also splitting logic?
            return findNode(DocumentKeyBuilder.of(key));
        }
        
        store.find(collection, key, maxCacheAge);
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        // TODO Auto-generated method stub

    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        // TODO Auto-generated method stub

    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Map<Key, Condition>> toRemove) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        // TODO Auto-generated method stub

    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        // TODO Auto-generated method stub

    }

    @Override
    public void dispose() {
        // TODO Auto-generated method stub

    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        // TODO Auto-generated method stub

    }

    @Override
    public CacheStats getCacheStats() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Map<String, String> getMetadata() {
        // TODO Auto-generated method stub
        return null;
    }
}
