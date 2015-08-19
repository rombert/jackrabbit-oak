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
package org.apache.jackrabbit.oak;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.Document;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.MultiplexingDocumentStore;
import org.apache.jackrabbit.oak.plugins.document.memory.MemoryDocumentStore;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.memory.MemoryStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * NodeStore fixture for parametrized tests.
 */
public abstract class NodeStoreFixture {

    public static final NodeStoreFixture SEGMENT_MK = new NodeStoreFixture() {
        @Override
        public String toString() {
            return "SegmentMK Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            return new SegmentNodeStore(new MemoryStore());
        }

        @Override
        public void dispose(NodeStore nodeStore) {
        }
    };

    public static final NodeStoreFixture MONGO_NS = new NodeStoreFixture() {
        @Override
        public String toString() {
            return "MongoNS Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            return new DocumentMK.Builder().getNodeStore();
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            if (nodeStore instanceof DocumentNodeStore) {
                ((DocumentNodeStore) nodeStore).dispose();
            }
        }
    };
    
    static class SpyingMemoryNodeStore extends MemoryDocumentStore {
        int calls;

        private final Logger log = LoggerFactory.getLogger(getClass());
        final String mountPath;
        
        SpyingMemoryNodeStore(String mountPath) {
            this.mountPath = mountPath;
        }
        
        void complainIfNoCalls() {
            if(calls == 0) {
                log.warn("{}: find not called at path {}", getClass().getSimpleName(), mountPath);
            }
        }
        
        @Override
        public <T extends Document> T find(Collection<T> collection,
                String key, int maxCacheAge) {
            calls++;
            return super.find(collection, key, maxCacheAge);
        }

        @Override
        public <T extends Document> T find(Collection<T> collection, String key) {
            calls++;
            return super.find(collection, key);
        }
        
        
    };

    public static final NodeStoreFixture MEMORY_MULTI_NS = new NodeStoreFixture() {
        private final SpyingMemoryNodeStore rootStore = new SpyingMemoryNodeStore("/");
        private final List<SpyingMemoryNodeStore> mounts = new ArrayList<SpyingMemoryNodeStore>();
        
        {
            //mounts.add(new SpyingMemoryNodeStore("/x"));
            //mounts.add(new SpyingMemoryNodeStore("/jcr:system"));
            //mounts.add(new SpyingMemoryNodeStore("/salut"));
        }
        
        @Override
        public String toString() {
            return "Multiplexing MemoryNodeStore Fixture";
        }

        @Override
        public NodeStore createNodeStore() {
            final MultiplexingDocumentStore.Builder b = new MultiplexingDocumentStore.Builder();
            b.root(rootStore);
            for(SpyingMemoryNodeStore s : mounts) {
                b.mount(s.mountPath, s);
            }
            return new DocumentMK.Builder().setDocumentStore(b.build()).getNodeStore();
        }

        @Override
        public void dispose(NodeStore nodeStore) {
            rootStore.complainIfNoCalls();
            for(SpyingMemoryNodeStore s : mounts) {
                s.complainIfNoCalls();
            }
            if (nodeStore instanceof DocumentNodeStore) {
                ((DocumentNodeStore) nodeStore).dispose();
            }
        }
    };

    public static final NodeStoreFixture MEMORY_NS = new NodeStoreFixture() {
        @Override
        public NodeStore createNodeStore() {
            return new MemoryNodeStore();
        }

        @Override
        public void dispose(NodeStore nodeStore) { }
    };

    public abstract NodeStore createNodeStore();

    public abstract void dispose(NodeStore nodeStore);

}
