/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static com.google.common.collect.ImmutableSet.copyOf;
import static com.google.common.collect.ImmutableSet.of;
import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.plugins.memory.PropertyStates.createProperty;
import static org.junit.Assert.assertEquals;

public class MultiplexingIndexStoreStrategyTest {

    @Test
    public void defaultSetup() throws Exception{
        MultiplexingIndexStoreStrategy store = new MultiplexingIndexStoreStrategy(new ContentMirrorStoreStrategy(),
                MountInfoProvider.DEFAULT);

        assertEquals(INDEX_CONTENT_NODE_NAME, store.getIndexNodeName("/foo"));
        assertEquals(INDEX_CONTENT_NODE_NAME, store.getNodeForMount(Mount.DEFAULT));
    }

    @Test
    public void customNodeName() throws Exception{
        MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                .mount("foo", "/a", "/b")
                .build();

        Mount m = mip.getMountByName("foo");
        MultiplexingIndexStoreStrategy store = new MultiplexingIndexStoreStrategy(new ContentMirrorStoreStrategy(),
                mip);

        assertEquals(":index", store.getIndexNodeName("/foo"));
        assertEquals(":index", store.getNodeForMount(Mount.DEFAULT));

        assertEquals(":" + m.getPathFragmentName() + "-index", store.getIndexNodeName("/a"));
        assertEquals(":" + m.getPathFragmentName() + "-index", store.getNodeForMount(m));
    }

    @Test
    public void nodeForMount() throws Exception{

    }

    @Test
    public void countDefault() throws Exception{
        ConfigurableStorageStrategy s = new TestStrategy() {
            @Override
            public long count(Filter filter, NodeState root, NodeState indexMeta,
                              String indexStorageNodeName, Set<String> values, int max) {
                throw new AssertionError();
            }

            @Override
            public long count(Filter filter, NodeState root, NodeState indexMeta,
                              Set<String> values, int max) {
                return 42;
            }
        };

        MultiplexingIndexStoreStrategy store = new MultiplexingIndexStoreStrategy(s, MountInfoProvider.DEFAULT);
        assertEquals(42, store.count(null, EMPTY_NODE, EMPTY_NODE, null, -1));
    }

    @Test
    public void countWithMount() throws Exception{
        final Set<String> nodeNames = new HashSet<String>();
        ConfigurableStorageStrategy s = new TestStrategy() {
            @Override
            public long count(Filter filter, NodeState root, NodeState indexMeta,
                              String indexStorageNodeName, Set<String> values, int max) {
                NodeState ns = indexMeta.getChildNode(indexStorageNodeName);
                nodeNames.add(ns.getString("name"));
                return ns.getLong("count");
            }

            @Override
            public long count(Filter filter, NodeState root, NodeState indexMeta,
                              Set<String> values, int max) {
                throw new AssertionError();
            }
        };

        MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                .mount("foo", "/a", "/b")
                .build();

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child(":a-index").setProperty("name",":a-index").setProperty("count", 5);
        builder.child(":b-index").setProperty("name",":b-index").setProperty("count", 7);
        builder.child("foo").setProperty("name","foo").setProperty("count", 42);

        MultiplexingIndexStoreStrategy store = new MultiplexingIndexStoreStrategy(s, mip);
        assertEquals(12, store.count(null, EMPTY_NODE, builder.getNodeState(), null, -1));
        assertEquals(of(":a-index",":b-index"), nodeNames);
    }

    @Test
    public void queryDefault() throws Exception{
        ConfigurableStorageStrategy s = new TestStrategy() {
            @Override
            public Iterable<String> query(Filter filter, String indexName, NodeState indexMeta, Iterable<String> values) {
                throw new AssertionError();
            }

            @Override
            public Iterable<String> query(Filter filter, String indexName, NodeState indexMeta,
                                          String indexStorageNodeName, Iterable<String> values) {
                return asList("/a", "/b");
            }
        };

        MultiplexingIndexStoreStrategy store = new MultiplexingIndexStoreStrategy(s, MountInfoProvider.DEFAULT);
        assertEquals(of("/a", "/b"), copyOf(store.query(null, "foo", EMPTY_NODE, null)));
    }


    @Test
    public void queryMount() throws Exception{
        final Set<String> nodeNames = new HashSet<String>();
        ConfigurableStorageStrategy s = new TestStrategy() {
            @Override
            public Iterable<String> query(Filter filter, String indexName, NodeState indexMeta, Iterable<String> values) {
                throw new AssertionError();
            }

            @Override
            public Iterable<String> query(Filter filter, String indexName, NodeState indexMeta,
                                          String indexStorageNodeName, Iterable<String> values) {
                NodeState ns = indexMeta.getChildNode(indexStorageNodeName);
                nodeNames.add(ns.getString("name"));
                return ns.getProperty("paths").getValue(Type.STRINGS);

            }
        };

        MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                .mount("foo", "/a", "/b")
                .build();

        NodeBuilder builder = EMPTY_NODE.builder();
        builder.child(":a-index")
                .setProperty("name",":a-index")
                .setProperty(createProperty("paths", of("/a", "/b"), Type.STRINGS));
        builder.child(":b-index")
                .setProperty("name",":b-index")
                .setProperty(createProperty("paths", of("/c", "/d"), Type.STRINGS));
        builder.child("foo")
                .setProperty("name","foo")
                .setProperty(createProperty("paths", of("/e", "/f"), Type.STRINGS));


        MultiplexingIndexStoreStrategy store = new MultiplexingIndexStoreStrategy(s, mip);
        assertEquals(of("/a", "/b", "/c", "/d"), copyOf(store.query(null, "foo", builder.getNodeState(), null)));
        assertEquals(of(":a-index",":b-index"), nodeNames);
    }


    private static class TestStrategy implements ConfigurableStorageStrategy {

        @Override
        public Iterable<String> query(Filter filter, String indexName, NodeState indexMeta,
                                      String indexStorageNodeName, Iterable<String> values) {
            return null;
        }

        @Override
        public long count(Filter filter, NodeState root, NodeState indexMeta,
                          String indexStorageNodeName, Set<String> values, int max) {
            return 0;
        }

        @Override
        public void update(NodeBuilder index, String path, String indexName,
                           NodeBuilder indexMeta, Set<String> beforeKeys, Set<String> afterKeys) {

        }

        @Override
        public boolean exists(NodeBuilder index, String key) {
            return false;
        }

        @Override
        public Iterable<String> query(Filter filter, String indexName,
                                      NodeState indexMeta, Iterable<String> values) {
            return null;
        }

        @Override
        public long count(NodeState root, NodeState indexMeta, Set<String> values, int max) {
            return 0;
        }

        @Override
        public long count(Filter filter, NodeState root, NodeState indexMeta, Set<String> values, int max) {
            return 0;
        }
    }

}