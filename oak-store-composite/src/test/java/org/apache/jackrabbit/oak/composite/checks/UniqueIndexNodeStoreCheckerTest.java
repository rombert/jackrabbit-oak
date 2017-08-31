/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite.checks;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;

import java.util.function.Consumer;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.IllegalRepositoryStateException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.composite.MountedNodeStore;
import org.apache.jackrabbit.oak.composite.checks.UniqueIndexNodeStoreChecker.Context;
import org.apache.jackrabbit.oak.plugins.index.IndexUpdateProvider;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.EditorHook;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mounts;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.ImmutableSet;

public class UniqueIndexNodeStoreCheckerTest {

    @Rule
    public final ExpectedException exception = ExpectedException.none();
    
    private MountInfoProvider mip;
    
    @Before
    public void prepare() {
        mip = Mounts.newBuilder().readOnlyMount("libs", "/libs", "/apps").build();
    }
    
    @Test
    public void uuidConflict() throws CommitFailedException {
        
        MemoryNodeStore globalStore = new MemoryNodeStore();
        MemoryNodeStore mountedStore = new MemoryNodeStore();
        
        populateStore(globalStore, b  -> b.child("first").setProperty("foo", "bar"));
        populateStore(mountedStore, b -> b.child("libs").child("first").setProperty("foo", "bar"));
        
        dump(globalStore.getRoot());
        dump(mountedStore.getRoot());
        
        UniqueIndexNodeStoreChecker checker = new UniqueIndexNodeStoreChecker();
        Context ctx = checker.createContext(globalStore, mip);
        
        exception.expect(IllegalRepositoryStateException.class);
        exception.expectMessage("1 errors were found");
        exception.expectMessage("clash for value bar: 'duplicate unique index entry'");
        
        ErrorHolder error = new ErrorHolder();
        checker.check(new MountedNodeStore(mip.getMountByName("libs"), mountedStore), TreeFactory.createReadOnlyTree(mountedStore.getRoot()), error, ctx);
        error.end();
    }
    
    private void dump(NodeState root) {
        System.out.println("--------");
        dump0(root, 0);
        System.out.println("--------");
    }

    private void dump0(NodeState node, int indent) {
        for ( PropertyState prop: node.getProperties() ) {
            for (int i = 0 ; i < indent; i++ ) 
                System.out.print(' ');
            
            String val = prop.isArray() ? prop.getValue(Type.STRINGS).toString() : prop.getValue(Type.STRING);
            
            System.out.format("+ %s: %s%n", prop.getName(), val);
        }
        
        for ( ChildNodeEntry child : node.getChildNodeEntries() ) {
            for (int i = 0 ; i < indent; i++ ) 
                System.out.print(' ');
            System.out.format("- %s%n", child.getName());
            
            dump0(child.getNodeState(), indent + 2);
        }
        
    }

    private void populateStore(NodeStore ns, Consumer<NodeBuilder> action) throws CommitFailedException {
        
        NodeBuilder builder = ns.getRoot().builder();
        NodeBuilder index = createIndexDefinition(builder.child(INDEX_DEFINITIONS_NAME), "foo",
                true, true, ImmutableSet.of("foo"), null);
        index.setProperty("entryCount", -1);   
        
        action.accept(builder);
        
        ns.merge(builder,new EditorHook(new IndexUpdateProvider(
                new PropertyIndexEditorProvider().with(mip))), CommitInfo.EMPTY);
    }
}
