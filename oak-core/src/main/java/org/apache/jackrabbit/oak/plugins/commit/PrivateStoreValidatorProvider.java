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

package org.apache.jackrabbit.oak.plugins.commit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.Tracker;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;

/**
 * {@link Validator} which detects change commits to the read only mounts.
 */
public class PrivateStoreValidatorProvider extends ValidatorProvider {

    private Whiteboard whiteboard;
    private List<String> readOnlyRoots;

    /**
     * Use a {@link Whiteboard} to determine the system's read only mounts
     *
     * @param whiteboard - the {@link Whiteboard}
     */
    public PrivateStoreValidatorProvider(Whiteboard whiteboard) {
        this.whiteboard = whiteboard;
    }

    /**
     * Use a fixed set of read only root paths
     *
     * @param readOnlyRoots - a list of read only root paths
     */
    public PrivateStoreValidatorProvider(String... readOnlyRoots) {
        this.readOnlyRoots = Arrays.asList(readOnlyRoots);
    }

    @Nonnull
    public Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        return new PrivateStoreValidator();
    }

    /**
     * Gets a {@link MountInfoProvider} service.
     *
     * @return a {@link MountInfoProvider} based on the fixed set of read only root paths, if any.
     * Otherwise the {@link Whiteboard} will be used to attempt the resolution of a {@link MountInfoProvider}
     * If all other methods fail, the {@code MountInfoProvider.DEFAULT} will be returned as a default.
     */
    private MountInfoProvider getMountInfoProvider() {
        MountInfoProvider mountInfoProvider = MountInfoProvider.DEFAULT;

        if (readOnlyRoots != null
                && readOnlyRoots.size() > 0) {

            mountInfoProvider = SimpleMountInfoProvider.newBuilder()
                .readOnlyMount("readonly", readOnlyRoots.toArray(new String[0]))
                .build();
        } else if (whiteboard != null) {
            Tracker<MountInfoProvider> mountInfoProviderTracker = whiteboard.track(MountInfoProvider.class);
            List<MountInfoProvider> mountInfoProviderList = mountInfoProviderTracker.getServices();

            if (mountInfoProviderList.size() > 0) {
                mountInfoProvider = mountInfoProviderList.get(0);
            }

            mountInfoProviderTracker.stop();
        }


        return mountInfoProvider;
    }

    private class PrivateStoreValidator extends DefaultValidator {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        public PrivateStoreValidator() {
        }

        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            logReadOnlyMountCommit(name, after);
            return null;
        }

        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            logReadOnlyMountCommit(name, after);
            return null;
        }

        public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            logReadOnlyMountCommit(name, before);
            return null;
        }

        private void logReadOnlyMountCommit(String nodeName, NodeState nodeState) {
            Tree nodeStateTree = TreeFactory.createReadOnlyTree(nodeState);
            String nodePath = nodeStateTree.getPath();
            String changeNodePath = nodeStateTree.isRoot()
                    ? nodePath + nodeName
                    : nodePath + "/" + nodeName;

            MountInfoProvider mountInfoProvider = getMountInfoProvider();
            Mount mountInfo = mountInfoProvider.getMountByPath(changeNodePath);
            if (mountInfo.isReadOnly()) {
                logger.error("Detected commit to a read-only store! ", new Throwable("Commit path: " + changeNodePath));
            }
        }
    }
}
