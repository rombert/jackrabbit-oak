/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static com.google.common.base.Suppliers.memoize;
import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.index.property.strategy.IndexStoreStrategy.IndexEntry;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.base.Supplier;

@RunWith(Parameterized.class)
public class IndexStoreStrategySharedTest {
    
    private static final Set<String> EMPTY = newHashSet();
    private String indexName;
    private NodeBuilder indexMeta;
    private IndexStoreStrategy store;
    
    @Parameterized.Parameters(name="{0}")
    public static Collection<Object[]> fixtures() {
        return Arrays.asList(new Object[][] {
                new Object[] { new UniqueEntryStoreStrategy() }
        });
    }
    
    public IndexStoreStrategySharedTest(IndexStoreStrategy store) {
        this.store = store;
    }
    
    @Before
    public void fillIndex() throws Exception {
        indexName = "foo";
        
        NodeState root = EMPTY_NODE;
        indexMeta = root.builder();
        Supplier<NodeBuilder> index = memoize(() -> indexMeta.child(INDEX_CONTENT_NODE_NAME));
        store.update(index, "/some/node1", null, null, EMPTY, newHashSet("key1"));
        store.update(index, "/some/node2", null, null, EMPTY, newHashSet("key2"));
    }

    @Test
    public void queryEntries_All() {
        
        Iterable<IndexEntry> hits = store.queryEntries(FilterImpl.newTestInstance(), indexName, indexMeta.getNodeState(), null);
        
        assertThat(hits, containsInAnyOrder(new IndexEntry("/some/node1", "key1"), new IndexEntry("/some/node2", "key2")));
    }
    
    @Test
    public void queryEntries_some() {

        Iterable<IndexEntry> hits = store.queryEntries(FilterImpl.newTestInstance(), indexName, indexMeta.getNodeState(), Arrays.asList("key1"));
        
        assertThat(hits, containsInAnyOrder(new IndexEntry("/some/node1", "key1")));
    }
}
