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
import com.google.common.collect.Maps;

import aQute.bnd.annotation.ConsumerType;

/**
 * The <tt>MultiplexingDocumentStore</tt> wraps two or more <tt>DocumentStore</tt> instances
 * 
 */
@ConsumerType
public class MultiplexingDocumentStore implements DocumentStore {
    
    private final Logger log = LoggerFactory.getLogger(getClass());
    
    private final List<DocumentStoreMount> mounts;
    
    private MultiplexingDocumentStore(List<DocumentStoreMount> mounts) {
        this.mounts = mounts;
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
        
        for ( DocumentStoreMount mount : mounts ) {
            if ( "/".equals(mount.getMountPath())) {
                return mount.getStore();
            }
        }
        
        throw new IllegalStateException("No root store configured");
    }


    private DocumentStore findNodeOwnerStore(DocumentKey key) {
        
        String path = key.getPath();
        List<DocumentStoreMount> candidates = Lists.newArrayList();

        // pick stores which can contribute
        for ( DocumentStoreMount mount : mounts ) {
            if ( Text.isDescendantOrEqual(mount.getMountPath(), path)) {
                candidates.add(mount);
            }
        }
        
        // sort candidates, longest paths first
        Collections.sort(candidates, new Comparator<DocumentStoreMount>() {
            @Override
            public int compare(DocumentStoreMount o1, DocumentStoreMount o2) {
                return o2.getMountPath().length() - o1.getMountPath().length();
            }
        });

        DocumentStoreMount bestMatch = candidates.get(0);
        
        log.info("For path {} selected store {} mounted at {}", key, 
                bestMatch.getStore(), bestMatch.getMountPath());

        return bestMatch.getStore();
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        return find(collection, key, Integer.MAX_VALUE);
    }
    
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {

        if ( collection != Collection.NODES ) {
            return rootStore().query(collection, fromKey, toKey, limit);
        }
        
        DocumentKey from = DocumentKeyImpl.fromKey(fromKey);
        DocumentKey to = DocumentKeyImpl.fromKey(toKey);
        
        DocumentStore owner = findNodeOwnerStore(from);
        List<T> main = owner.query(collection, fromKey, toKey, indexedProperty, startValue, limit);
        // TODO - do we need a query on the contributing stores or is a 'find' enough?
        for ( DocumentStore contributing : findStoresContainedBetween(from, to)) {
            // TODO - stop the query if we know that we have enough results, e.g. we
            // have hit the limit with results between fromKey and contributing.getMountPath()  
            main.addAll(contributing.query(collection, fromKey, toKey, indexedProperty, startValue, limit));
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
        for ( DocumentStoreMount mount : mounts ) {
            String storePath = mount.getMountPath();
            if ( from.getPath().compareTo(storePath) < 0 && storePath.compareTo(to.getPath()) < 0 ) {
                contained.add(mount.getStore());
            }
        }
        return contained;
    }
    
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey, int limit) {
        return query(collection, fromKey, toKey, null, 0, limit);
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, String key) {
        remove(collection, Collections.singletonList(key));
    }

    @Override
    public <T extends Document> void remove(Collection<T> collection, List<String> keys) {
        if ( collection != Collection.NODES ) {
            rootStore().remove(collection, keys);
            return;
        }
        
        for ( String key : keys ) {
            findNodeOwnerStore(DocumentKeyImpl.fromKey(key)).remove(collection, key);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Map<Key, Condition>> toRemove) {
        if ( collection != Collection.NODES) {
            return rootStore().remove(collection, toRemove);
        }
        
        // map each owner to store to the specific removals it will handle
        Map<DocumentStore, Map<String, Map<Key, Condition>>> storesToRemovals = Maps.newHashMap();
        
        for ( Map.Entry<String, Map<Key, Condition>> entry : toRemove.entrySet()) {
            
            DocumentStore ownerStore = findNodeOwnerStore(DocumentKeyImpl.fromKey(entry.getKey()));
            
            Map<String, Map<Key, Condition>> removals = storesToRemovals.get(ownerStore);
            if ( removals == null ) {
                removals = Maps.newHashMap();
                storesToRemovals.put(ownerStore, removals);
            }
            
            removals.put(entry.getKey(), entry.getValue());
        }
        
        int removals = 0;
        
        // process removals for each store
        for ( Map.Entry<DocumentStore, Map<String, Map<Key, Condition>>> entry : storesToRemovals.entrySet()  ) {
           removals += entry.getKey().remove(collection, entry.getValue());
        }
        
        return removals;
    }

    @Override
    public <T extends Document> boolean create(Collection<T> collection, List<UpdateOp> updateOps) {
        if ( collection != Collection.NODES) {
            return rootStore().create(collection, updateOps);
        }
        
        boolean created = false;
        
        for ( UpdateOp updateOp: updateOps ) {
           DocumentKey key = DocumentKeyImpl.fromKey(updateOp.getId());
           created |= findNodeOwnerStore(key).create(collection, Collections.singletonList(updateOp));
        }
        
        return created;
    }

    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        for ( String key : keys) {
            findNodeOwnerStore(DocumentKeyImpl.fromKey(key)).update(collection, Collections.singletonList(key), updateOp);
        }
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        
        return findNodeOwnerStore(DocumentKeyImpl.fromKey(update.getId())).createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        return findNodeOwnerStore(DocumentKeyImpl.fromKey(update.getId())).findAndUpdate(collection, update);
    }

    @Override
    public CacheInvalidationStats invalidateCache() {
        for ( DocumentStoreMount mount : mounts ) {
            mount.getStore().invalidateCache();
        }
        // TODO return aggregate stats
        return null;
    }

    @Override
    public <T extends Document> void invalidateCache(Collection<T> collection, String key) {
        
        if ( collection != Collection.NODES ) {
            rootStore().invalidateCache();
            return;
        }
        
        findNodeOwnerStore(DocumentKeyImpl.fromKey(key)).invalidateCache(collection, key);
    }
    
    @Override
    public CacheInvalidationStats invalidateCache(Iterable<String> keys) {
        // TODO optimize
        return invalidateCache();
    }

    @Override
    public void dispose() {
        for ( DocumentStoreMount mount : mounts ) {
            mount.getStore().dispose();
        }
    }

    @Override
    public <T extends Document> T getIfCached(Collection<T> collection, String key) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void setReadWriteMode(String readWriteMode) {
        for ( DocumentStoreMount mount : mounts ) {
            mount.getStore().setReadWriteMode(readWriteMode);
        }
    }

    @Override
    public CacheStats getCacheStats() {
        // TODO return aggregate stats
        return null;
    }

    @Override
    public Map<String, String> getMetadata() {
        // TODO return aggregate metadata?
        return null;
    }
    
    public static class Builder {
        
        private List<DocumentStoreMount> mounts = Lists.newArrayList();
        private boolean hasRoot;
        
        public Builder root(DocumentStore store) {
            
            mounts.add(new DocumentStoreMount(store, "/"));
            
            hasRoot = true;
            
            return this;
        }
        
        public Builder mount(String path, DocumentStore store) {
            
            // TODO - check path is absolute and maybe delegate to root() is path is '/'
            // TODO - check for duplicates

            mounts.add(new DocumentStoreMount(store, path));
            
            return this;
        }
        
        public MultiplexingDocumentStore build() {
            
            Preconditions.checkArgument(hasRoot, "No %s instance mounted at '/'", DocumentStore.class.getSimpleName());
            
            Preconditions.checkArgument(mounts.size() > 1, 
                    "Expected at least 2 %s instances but got %s.", DocumentStore.class.getSimpleName(), mounts.size());
            
            return new MultiplexingDocumentStore(mounts); 
        }
    }
    
    private static class DocumentStoreMount {
        private final DocumentStore store;
        private final String mountPath;

        public DocumentStoreMount(DocumentStore store, String mountPath) {
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
