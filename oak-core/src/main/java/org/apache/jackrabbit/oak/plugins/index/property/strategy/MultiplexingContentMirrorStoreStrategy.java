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

import java.util.List;
import java.util.Set;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

public class MultiplexingContentMirrorStoreStrategy implements IndexStoreStrategy {
    private final ContentMirrorStoreStrategy strategy;
    private final MountInfoProvider mountInfoProvider;

    public MultiplexingContentMirrorStoreStrategy(ContentMirrorStoreStrategy strategy,
                                                  MountInfoProvider mountInfoProvider) {
        this.strategy = strategy;
        this.mountInfoProvider = mountInfoProvider;
    }

    @Override
    public void update(NodeBuilder index, String path, String indexName, NodeBuilder indexMeta,
                       Set<String> beforeKeys, Set<String> afterKeys) {
        strategy.update(index, path, indexName, indexMeta, beforeKeys, afterKeys);
    }

    @Override
    public boolean exists(NodeBuilder index, String key) {
        return strategy.exists(index, key);
    }

    @Override
    public Iterable<String> query(Filter filter, String indexName, NodeState indexMeta,
                                  Iterable<String> values) {
        if (mountInfoProvider.getNonDefaultMounts().isEmpty()){
            return strategy.query(filter, indexName, indexMeta, values);
        }

        List<Iterable<String>> iterables = Lists.newArrayList();
        for(ChildNodeEntry cne : indexMeta.getChildNodeEntries()){
            String name = cne.getName();
            if (NodeStateUtils.isHidden(name) && name.endsWith("index")){
                iterables.add(strategy.query(filter, indexName, indexMeta, name, values));
            }
        }
        return Iterables.concat(iterables);
    }

    @Override
    public long count(NodeState root, NodeState indexMeta, Set<String> values, int max) {
        return strategy.count(root, indexMeta, values, max);
    }

    @Override
    public long count(Filter filter, NodeState root, NodeState indexMeta, Set<String> values, int max) {
        return strategy.count(filter, root, indexMeta, values, max);
    }
}
