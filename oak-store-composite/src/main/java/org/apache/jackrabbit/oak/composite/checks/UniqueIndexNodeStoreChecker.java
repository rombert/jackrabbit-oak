package org.apache.jackrabbit.oak.composite.checks;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.UNIQUE_PROPERTY_NAME;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.plugins.index.property.Multiplexers;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Maps;

public class UniqueIndexNodeStoreChecker implements MountedNodeStoreChecker<UniqueIndexNodeStoreChecker.Context> {

    @Override
    public Context createContext(NodeStore globalStore, MountInfoProvider mip) {

        Context ctx = new Context(mip);
        
        // read definitions from oak:index, and pick all unique indexes
        NodeState indexDefs = globalStore.getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        for ( ChildNodeEntry indexDef : indexDefs.getChildNodeEntries() ) {
            if ( indexDef.getNodeState().hasProperty(UNIQUE_PROPERTY_NAME) &&
                    indexDef.getNodeState().getBoolean(UNIQUE_PROPERTY_NAME) ) {
                ctx.add(indexDef, 
                        Mounts.defaultMountInfoProvider().getDefaultMount(), indexDef.getNodeState());
            }
        }
        
        return ctx;
    }
    
    private static String describe(Mount mount) {
        return mount.isDefault() ? "default mount" : mount.getName();
    }
    

    @Override
    public void check(MountedNodeStore mountedStore, Tree tree, ErrorHolder errorHolder, Context context) {
        
        // gather index definitions owned by this mount
        NodeState indexDefs = mountedStore.getNodeStore().getRoot().getChildNode(INDEX_DEFINITIONS_NAME);
        
        for ( ChildNodeEntry indexDef : indexDefs.getChildNodeEntries() ) {
            if ( indexDef.getNodeState().hasProperty(UNIQUE_PROPERTY_NAME) &&
                    indexDef.getNodeState().getBoolean(UNIQUE_PROPERTY_NAME) ) {
                
                String mountIndexDefName = Multiplexers.getNodeForMount(mountedStore.getMount(), indexDef.getName());
                
                NodeState mountIndexDef = indexDef.getNodeState().getChildNode(mountIndexDefName);
                if ( mountIndexDef.exists() ) {
                    context.add(indexDef, 
                           mountedStore.getMount(), indexDef.getNodeState());
                    
                }
            }
        }

        // execute checks
        
        context.runChecks(context, errorHolder);
    }
    
    static class Context {
        private final MountInfoProvider mip;
        private final Map<String, IndexCombination> combinations = new HashMap<>();
        
        Context(MountInfoProvider mip) {
            this.mip = mip;
        }

        public void add(ChildNodeEntry rootIndexDef, Mount mount, NodeState indexDef) {
            
            IndexCombination combination = combinations.putIfAbsent(rootIndexDef.getName(), new IndexCombination(rootIndexDef));
            
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
        
        IndexCombination(ChildNodeEntry rootIndexDef) {
            this.rootIndexDef = rootIndexDef;
        }
        
        public void addEntry(Mount mount, NodeState indexDef) {
            
            if ( indexEntries.containsKey(mount) ) {
                throw new IllegalArgumentException("Index definition for " + rootIndexDef.getName() + " already contains information for mount " + describe(mount));
            }
            
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
            
            for ( Mount[] checkedEntry : checked ) {
                if ( checkedEntry[0].equals(first) && checkedEntry[1].equals(second) ) {
                    return true;
                }
            }
            
            return true;
        }
        
        private void recordChecked(Mount first, Mount second) {
            
            checked.add(new Mount[] { first, second });
        }
        
        private void check(Entry<Mount, NodeState> indexEntry, Entry<Mount, NodeState> indexEntry2, Context ctx, ErrorHolder errorHolder) {
            
            Set<IndexStoreStrategy> strategies = Multiplexers.getStrategies(true, ctx.getMountInfoProvider(), indexEntry.getValue(), rootIndexDef.getName());
            Set<IndexStoreStrategy> strategies2 = Multiplexers.getStrategies(true, ctx.getMountInfoProvider(), indexEntry2.getValue(), rootIndexDef.getName());
            for ( IndexStoreStrategy strategy : strategies ) {
                for ( String indexHit : strategy.query(Filter.EMPTY_FILTER, rootIndexDef.getName(), rootIndexDef.getNodeState(), null) ) {
                    for ( IndexStoreStrategy strategy2 : strategies2 ) {
                        Iterable<String> result = strategy2.query(Filter.EMPTY_FILTER, rootIndexDef.getName(), rootIndexDef.getNodeState(), Collections.singleton(indexHit));
                        if ( result.iterator().hasNext() ) {
                            // TODO - report both mounts
                            errorHolder.report(new MountedNodeStore(indexEntry.getKey(), null), indexHit, "duplicate index entry");
                        }
                    }
                }
            }
        }
    }
}
