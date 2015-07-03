package org.apache.jackrabbit.oak.plugins.document;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import aQute.bnd.annotation.ConsumerType;

/**
 * The <tt>MultiplexingDocumentStore</tt> wraps two or more <tt>DocumentStore</tt> instances
 * 
 */
@ConsumerType
public class MultiplexingDocumentStore implements DocumentStore {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final List<MountedDocumentStore> stores;
    
    private MultiplexingDocumentStore(List<MountedDocumentStore> stores) {
        this.stores = stores;
    }
    
    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {

        if ( collection == Collection.NODES) {
            return findNode(DocumentKeyImpl.fromKey(key));
        }
        
        return rootStore().find(collection, key);
    }
    
    private <T extends Document> T findNode(DocumentKey key) {
        
        DocumentStore store = findNodeOwnerStore(key);
        
        // TODO - can we get rid of the cast? perhaps return NodeDocument
        return (T) store.find(Collection.NODES, key.getValue());
    }
    
    private DocumentStore rootStore() {
        
        for ( MountedDocumentStore store : stores ) {
            if ( "/".equals(store.getMountPath())) {
                return store.getStore();
            }
        }
        
        throw new IllegalStateException("No root store configured");
    }


    private DocumentStore findNodeOwnerStore(DocumentKey key) {
        
        String path = key.getPath();
        List<MountedDocumentStore> candidates = Lists.newArrayList();

        // pick stores which can contribute
        for ( MountedDocumentStore store : stores ) {
            if ( Text.isDescendantOrEqual(store.getMountPath(), path)) {
                candidates.add(store);
            }
        }
        
        // sort candidates, longest paths first
        Collections.sort(candidates, new Comparator<MountedDocumentStore>() {
            @Override
            public int compare(MountedDocumentStore o1, MountedDocumentStore o2) {
                return o2.getMountPath().length() - o1.getMountPath().length();
            }
        });

        MountedDocumentStore bestMatch = candidates.get(0);
        
        log.info("For path {} selected store {} mounted at {}", key, 
                bestMatch.getStore(), bestMatch.getMountPath());

        return bestMatch.getStore();
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        
        if ( collection != Collection.NODES ) {
            return rootStore().query(collection, fromKey, toKey, limit);
        }
        
        DocumentKey from = DocumentKeyImpl.fromKey(fromKey);
        DocumentKey to = DocumentKeyImpl.fromKey(toKey);
        
        DocumentStore owner = findNodeOwnerStore(from);
        List<T> main = owner.query(collection, fromKey, toKey, limit);
        // TODO - do we need a query on the contributing stores or is a 'find' enough?
        for ( DocumentStore contributing : findStoresContainedBetween(from, to)) {
            // TODO - stop the query if we know that we have enough results, e.g. we
            // have hit the limit with results between fromKey and contributing.getMountPath()  
            main.addAll(contributing.query(collection, fromKey, toKey, limit));
        }
        
        // TODO - merge the results instead of full sorting
        Collections.sort(main, new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                return o1.getId().compareTo(o2.getId());
            }
        });
        
        return main.size() > limit ? main.subList(0, limit) : main;
    }
    

    private List<DocumentStore> findStoresContainedBetween(DocumentKey from, DocumentKey to) {
        
        List<DocumentStore> contained = Lists.newArrayList();
        for ( MountedDocumentStore mountedStore : stores ) {
            String storePath = mountedStore.getMountPath();
            if ( from.getPath().compareTo(storePath) < 0 && storePath.compareTo(to.getPath()) > 0 ) {
                contained.add(mountedStore.getStore());
            }
        }
        return contained;
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
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        // TODO Auto-generated method stub
        return null;
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
    
    public static class Builder {
        
        private List<MountedDocumentStore> stores = Lists.newArrayList();
        private boolean hasRoot;
        
        public Builder root(DocumentStore store) {
            
            stores.add(new MountedDocumentStore(store, "/"));
            
            hasRoot = true;
            
            return this;
        }
        
        public Builder mount(String path, DocumentStore store) {
            
            // TODO - check path is absolute and maybe delegate to root() is path is '/'
            // TODO - check for duplicates

            stores.add(new MountedDocumentStore(store, path));
            
            return this;
        }
        
        public MultiplexingDocumentStore build() {
            
            Preconditions.checkArgument(hasRoot, "No %s instance mounted at '/'", DocumentStore.class.getSimpleName());
            
            Preconditions.checkArgument(stores.size() > 1, 
                    "Expected at least 2 %s instances but got %s.", DocumentStore.class.getSimpleName(), stores.size());
            
            return new MultiplexingDocumentStore(stores); 
        }
    }
    
    private static class MountedDocumentStore {
        private final DocumentStore store;
        private final String mountPath;

        public MountedDocumentStore(DocumentStore store, String mountPath) {
            this.store = store;
            this.mountPath = mountPath;
        }
        
        public DocumentStore getStore() {
            return store;
        }
        
        public String getMountPath() {
            return mountPath;
        }
    }
}
