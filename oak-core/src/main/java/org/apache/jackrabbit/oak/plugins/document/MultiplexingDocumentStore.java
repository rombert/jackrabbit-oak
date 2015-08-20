package org.apache.jackrabbit.oak.plugins.document;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.cache.CacheStats;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Condition;
import org.apache.jackrabbit.oak.plugins.document.UpdateOp.Key;
import org.apache.jackrabbit.oak.plugins.document.cache.CacheInvalidationStats;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.util.Text;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * The <tt>MultiplexingDocumentStore</tt> wraps two or more <tt>DocumentStore</tt> instances
 * 
 * <p>Multiplexing is performed only for the {@link Collection#NODES nodes} collection.</p>
 * 
 * <p>This document store implementation assumes that the keys for document nodes are of the
 * <tt>DEPTH:PATH</tt> format and uses that information to decide how to multiplex operations,
 * i.e. to which store a certain node document belongs.</p>
 * 
 * <p>This store contains one root store, which by default holds all the nodes, and at least
 * one sub-store, which holds all document nodes below certain paths. This concept is similar
 * to the Unix mounts, where a filesystem can be mounted below a certain point.</p>
 * 
 */
public class MultiplexingDocumentStore implements DocumentStore {
    
    private final DocumentStore root;
    
    private final List<DocumentStoreMount> mounts;

    private MultiplexingDocumentStore(DocumentStore root, List<DocumentStoreMount> mounts) {
        this.root = root;
        
        this.mounts = Lists.newArrayList();
        this.mounts.add(new DocumentStoreMount(root, "/"));
        this.mounts.addAll(mounts);
        
        for ( DocumentStoreMount mount : this.mounts ) {
            mount.getStore().setDocumentCreationCustomiser(new DefaultDocumentCreationCustomiser(this));
        }
    }
    
    @Override
    public <T extends Document> T find(Collection<T> collection, String key) {
        return find(collection, key, Integer.MAX_VALUE);
    }
    
    private boolean doNotMap(String path) {
        return path.startsWith("/oak:");
    }
    
    private DocumentKey asDocumentKey(String key) {
        if(Utils.isIdFromLongPath(key)) {
            // We'll need to find a way to select a store based on those hashed keys which
            // are created by Utils.getIdFromPath(...)
            throw new IllegalArgumentException("Cannot use hashed document key:" + key);
        }
        return DocumentKey.fromKey(key);
    }
    
    private DocumentStore findOwnerStore(String key, Collection<?> collection, OnFailure onFailure) {
        return findOwnerStore(asDocumentKey(key), collection, onFailure);
    }
    
    private DocumentStore findOwnerStore(UpdateOp update, Collection<?> collection, OnFailure onFailure) {
        String keyPath = update.getId();
        final UpdateOp.Operation op = update.getChanges().get(new UpdateOp.Key(NodeDocument.PATH,null));
        if(op != null) {
            // If a long path was transformed to a hash, use the original path here
            keyPath = op.value.toString();
        }
        if(doNotMap(keyPath)) {
            return root;
        }
        return findOwnerStore(update.splitFrom != null ? update.splitFrom : keyPath, collection, onFailure);
    }
    

    private DocumentStore findOwnerStore(DocumentKey key, Collection<?> collection, OnFailure onFailure) {
        
        if ( collection != Collection.NODES ) {
            return root;
        }
        
        String path = key.getPath();
        
        if(doNotMap(path)) {
            return root;
        }
        
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
        
        if ( candidates.isEmpty()) {
            
            if ( onFailure == OnFailure.CALL_FIND_FOR_MATCHING_KEY) {
                // split documents don't have reasonable id so we can't locate
                // a store for them beforehand ;
                for ( DocumentStoreMount mount : mounts ) {
                    if ( mount.getStore().find(Collection.NODES, key.getValue()) != null ) {
                        return mount.getStore();
                    }
                }
                
            }
            
            throw new IllegalArgumentException("Could not find an owning store for key " + key.getValue() + " ( matched path = " + key.getPath() + ")");
        }

        // guaranteed to have at least one candidate since we mount the root store at '/'
        return candidates.get(0).getStore();
    }

    @Override
    public <T extends Document> T find(Collection<T> collection, String key, int maxCacheAge) {
        
        return findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
            .find(collection, key, maxCacheAge);
    }
    
    @Override
    public <T extends Document> List<T> query(Collection<T> collection, String fromKey, String toKey,
            String indexedProperty, long startValue, int limit) {

        if ( collection != Collection.NODES ) {
            return root.query(collection, fromKey, toKey, limit);
        }
        
        DocumentKey from = asDocumentKey(fromKey);
        DocumentKey to = asDocumentKey(toKey);
        
        DocumentStore owner = findOwnerStore(from, collection, OnFailure.FAIL_FAST);
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
            root.remove(collection, keys);
            return;
        }
        
        for ( String key : keys ) {
            findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
                .remove(collection, key);
        }
    }

    @Override
    public <T extends Document> int remove(Collection<T> collection, Map<String, Map<Key, Condition>> toRemove) {
        if ( collection != Collection.NODES) {
            return root.remove(collection, toRemove);
        }
        
        // map each owner to store to the specific removals it will handle
        Map<DocumentStore, Map<String, Map<Key, Condition>>> storesToRemovals = Maps.newHashMap();
        
        for ( Map.Entry<String, Map<Key, Condition>> entry : toRemove.entrySet()) {
            
            DocumentStore ownerStore = findOwnerStore(entry.getKey(), collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY);
            
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

        boolean created = false;
        
        for ( UpdateOp updateOp: updateOps ) {
           created |= findOwnerStore(updateOp, collection, OnFailure.FAIL_FAST)
                   .create(collection, Collections.singletonList(updateOp));
        }
        
        return created;
    }
    
    @Override
    public <T extends Document> void update(Collection<T> collection, List<String> keys, UpdateOp updateOp) {
        
        for ( String key : keys) {
            findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
                .update(collection, Collections.singletonList(key), updateOp);
        }
    }

    @Override
    public <T extends Document> T createOrUpdate(Collection<T> collection, UpdateOp update) {
        
        return findOwnerStore(update, collection, OnFailure.FAIL_FAST)
                .createOrUpdate(collection, update);
    }

    @Override
    public <T extends Document> T findAndUpdate(Collection<T> collection, UpdateOp update) {
        
        return findOwnerStore(update, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
                .findAndUpdate(collection, update);
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
            root.invalidateCache();
            return;
        }
        
        findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY).invalidateCache(collection, key);
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
        if ( collection != Collection.NODES) {
            return root.getIfCached(collection, key);
        }

        return findOwnerStore(key, collection, OnFailure.CALL_FIND_FOR_MATCHING_KEY)
                .getIfCached(collection, key);
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
        return Collections.emptyMap();
    }
    
    @Override
    public void setDocumentCreationCustomiser(DocumentCreationCustomiser customiser) {
        // TODO - forward to mounts?
        throw new UnsupportedOperationException();
    }
    
    /**
     * Helper class used to create <tt>MultiplexingDocumentStore</tt> instances
     * 
     * <p>It is required to set at least the {@link #root(DocumentStore) root document store} 
     * and one {@link #mount(String, DocumentStore) mount} before {@link #build() building}
     * the instance.</p>
     *
     */
    public static class Builder {
        
        private DocumentStore root;
        private List<DocumentStoreMount> mounts = Lists.newArrayList();
        
        public Builder root(DocumentStore store) {
            
            root = checkNotNull(store); 
            
            return this;
        }
        
        public Builder mount(String path, DocumentStore store) {
            
            // TODO - check for duplicate mounts?
            
            checkNotNull(store);
            checkNotNull(path);
            if ( !Text.isDescendant("/", path)) {
                throw new IllegalArgumentException("Invalid mount path '" + path +"'");
            }
            
            
            mounts.add(new DocumentStoreMount(store, path));
            
            return this;
        }
        
        public MultiplexingDocumentStore build() {
            
            Preconditions.checkArgument(root != null, "No %s instance mounted at '/'", DocumentStore.class.getSimpleName());
            
            Preconditions.checkArgument(mounts.size() > 0, 
                    "Expected at least 1 mounts but got %s.", mounts.size());
            
            return new MultiplexingDocumentStore(root, mounts); 
        }
    }
    
    /**
     * Private abstraction to simplify storing information about mounts
     */
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
    
    /**
     * Policy which indicates what action should be taken if a document's owning store can't be identified from the path
     * 
     * <p>Even if the DocumentNodeStore generally has a hierarchical usage pattern, some operations ( e.g. splitting documents ) 
     * make use of non-hierarchical identifiers.</p>
     *
     */
    private enum OnFailure {
        
        FAIL_FAST, CALL_FIND_FOR_MATCHING_KEY;
    }

}
