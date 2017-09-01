package org.apache.jackrabbit.oak.composite.checks;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy.IndexEntry;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

@Component
@Service(MountedNodeStoreChecker.class)
public class UniqueIndexNodeStoreChecker implements MountedNodeStoreChecker<UniqueIndexNodeStoreChecker.Context> {
    
    @Override
    public Context createContext(NodeStore globalStore, MountInfoProvider mip) {

        Context ctx = new Context(mip);
        
        // read definitions from oak:index, and pick all unique indexes
        NodeState indexDefs = globalStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        for ( ChildNodeEntry indexDef : indexDefs.getChildNodeEntries() ) {
            if ( indexDef.getNodeState().hasProperty(UNIQUE_PROPERTY_NAME) &&
                    indexDef.getNodeState().getBoolean(UNIQUE_PROPERTY_NAME) ) {
                ctx.add(indexDef, mip.getDefaultMount(), indexDefs);
                ctx.track(new MountedNodeStore(mip.getDefaultMount() , globalStore));
            }
        }
        
        return ctx;
    }
    
    @Override
    public boolean check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, Context context) {

        context.track(mountedStore);
        
        // TODO - only access when tree path is /oak:index, return true for /, false for others
        
        // gather index definitions owned by this mount
        NodeState indexDefs = mountedStore.getNodeStore().getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        
        for ( ChildNodeEntry indexDef : indexDefs.getChildNodeEntries() ) {
            if ( indexDef.getNodeState().hasProperty(UNIQUE_PROPERTY_NAME) &&
                    indexDef.getNodeState().getBoolean(UNIQUE_PROPERTY_NAME) ) {
                
                String mountIndexDefName = Multiplexers.getNodeForMount(mountedStore.getMount(), INDEX_CONTENT_NODE_NAME);
                
                NodeState mountIndexDef = indexDef.getNodeState().getChildNode(mountIndexDefName);
                
                // TODO - does it make sense to just restructure to hold the index definition and the root index nodes for all mounts?
                if ( mountIndexDef.exists() ) {
                    context.add(indexDef, mountedStore.getMount(), indexDefs);
                }
            }
        }

        // execute checks
        
        context.runChecks(context, errorHolder);
        
        return false;
    }
    
    static class Context {
        private final MountInfoProvider mip;
        private final Map<String, IndexCombination> combinations = new HashMap<>();
        private final Map<String, MountedNodeStore> mountedNodeStoresByName = Maps.newHashMap();
        
        Context(MountInfoProvider mip) {
            this.mip = mip;
        }

        public void track(MountedNodeStore mountedNodeStore) {
            mountedNodeStoresByName.put(mountedNodeStore.getMount().getName(), mountedNodeStore);
        }

        public void add(ChildNodeEntry rootIndexDef, Mount mount, NodeState indexDef) {

            IndexCombination combination = combinations.get(rootIndexDef.getName());
            if ( combination == null ) {
                combination = new IndexCombination(rootIndexDef);
                combinations.put(rootIndexDef.getName(), combination);
            }
            
            combination.addEntry(mount, indexDef);
        }
        
        public MountInfoProvider getMountInfoProvider() {
            
            return mip;
        }
        
        public void runChecks(Context context, ErrorHolder errorHolder) {
            for ( IndexCombination combination: combinations.values() ) {
                combination.runCheck(context, errorHolder);
            }
        }
    }
    
    static class IndexCombination {
        private final ChildNodeEntry rootIndexDef;
        private final Map<Mount, NodeState> indexEntries = Maps.newHashMap();
        private final List<Mount[]> checked = new ArrayList<>();
        private final Set<String> reportedConflictingValues = new HashSet<>();
        
        IndexCombination(ChildNodeEntry rootIndexDef) {
            this.rootIndexDef = rootIndexDef;
        }
        
        public void addEntry(Mount mount, NodeState indexDef) {
            
            if ( !indexEntries.containsKey(mount) )
                indexEntries.put(mount, indexDef);
        }
        
        public void runCheck(Context context, ErrorHolder errorHolder) {
            
            for ( Map.Entry<Mount, NodeState> indexEntry : indexEntries.entrySet() ) {
                for ( Map.Entry<Mount, NodeState> indexEntry2 : indexEntries.entrySet() ) {
                    
                    // same entry, skip
                    if ( indexEntry.getKey().equals(indexEntry2.getKey()) ) {
                        continue;
                    }
                    
                    if ( wasChecked(indexEntry.getKey(), indexEntry2.getKey() )) {
                        continue;
                    }
                    
                    check(indexEntry, indexEntry2, context, errorHolder);
                    
                    recordChecked(indexEntry.getKey(), indexEntry2.getKey());
                }
            }
        }

        private boolean wasChecked(Mount first, Mount second) {

            // TODO - better cross-check support - maybe just generate all combinations?
            for ( Mount[] checkedEntry : checked ) {
                if ( ( checkedEntry[0].equals(first) && checkedEntry[1].equals(second) ) ||
                        ( checkedEntry[1].equals(first) && checkedEntry[0].equals(second))) {
                    return true;
                }
            }
            
            return false;
        }
        
        private void recordChecked(Mount first, Mount second) {
            
            checked.add(new Mount[] { first, second });
        }
        
        private void check(Entry<Mount, NodeState> indexEntry, Entry<Mount, NodeState> indexEntry2, Context ctx, ErrorHolder errorHolder) {
            
            MountedNodeStore mountedNodeStore = ctx.mountedNodeStoresByName.get(indexEntry.getKey().getName());
            MountedNodeStore mountedNodeStore2 = ctx.mountedNodeStoresByName.get(indexEntry2.getKey().getName());
            
            NodeState indexDefs = indexEntry.getValue();
            NodeState indexDefs2 = indexEntry2.getValue();
            
            String indexName = rootIndexDef.getName();
            
            NodeState indexNode = indexDefs.getChildNode(indexName);
            NodeState indexNode2 = indexDefs2.getChildNode(indexName);
            
            Set<IndexStoreStrategy> strategies = Multiplexers.getStrategies(true, indexEntry.getKey().isDefault() ? Mounts.defaultMountInfoProvider() : ctx.getMountInfoProvider(), indexNode, INDEX_CONTENT_NODE_NAME);
            Set<IndexStoreStrategy> strategies2 = Multiplexers.getStrategies(true, indexEntry2.getKey().isDefault() ? Mounts.defaultMountInfoProvider() : ctx.getMountInfoProvider(), indexNode2, INDEX_CONTENT_NODE_NAME);

            // TODO - will be very slow for large indexes, will need to write entries to file, sort and compare - see the VersionGarbageCollector implementation - ExternalSort, StringSort
            for ( IndexStoreStrategy strategy : strategies ) {
                for ( IndexEntry hit : strategy.queryEntries(Filter.EMPTY_FILTER, indexName, indexNode, null) ) {
                    if ( !indexEntry.getKey().isMounted(hit.getPath() )) {
                        continue;
                    }
                    for ( IndexStoreStrategy strategy2 : strategies2 ) {
                        Iterable<IndexEntry> result = strategy2.queryEntries(Filter.EMPTY_FILTER, indexName, indexNode2, Collections.singleton(hit.getPropertyValue()));
                        if ( result.iterator().hasNext() ) {
                            IndexEntry hit2 = result.iterator().next();
                            if ( !indexEntry2.getKey().isMounted(hit2.getPath())) {
                                continue;
                            }
                            if ( reportedConflictingValues.add(hit.getPropertyValue())) {
                                errorHolder.report(mountedNodeStore, hit.getPath(), mountedNodeStore2, hit2.getPath(), 
                                        hit.getPropertyValue(), "duplicate unique index entry");
                            }
                        }
                    }
                }
            }
        }
    }
}
