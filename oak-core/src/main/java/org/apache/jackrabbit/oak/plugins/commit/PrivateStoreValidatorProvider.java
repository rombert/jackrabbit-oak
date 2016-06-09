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
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/**
 * {@link Validator} which detects change commits to the read only mounts.
 */
public class PrivateStoreValidatorProvider extends ValidatorProvider {

    private MountInfoProvider mountInfoProvider;
    private boolean failOnReadonlyMountCommit;

    /**
     * Use a {@link MountInfoProvider} to determine the system's read only mounts
     *
     * @param mountInfoProvider - the {@link MountInfoProvider}
     * @param failOnReadonlyMountCommit - fail when detecting a commit on a read only mount
     *  or just log the commit information and let the commit go through
     */
    public PrivateStoreValidatorProvider(MountInfoProvider mountInfoProvider, boolean failOnReadonlyMountCommit) {
        this.mountInfoProvider = mountInfoProvider;
        this.failOnReadonlyMountCommit = failOnReadonlyMountCommit;
    }

    @Nonnull
    public Validator getRootValidator(NodeState before, NodeState after, CommitInfo info) {
        return new PrivateStoreValidator();
    }

    private class PrivateStoreValidator extends DefaultValidator {
        private final Logger logger = LoggerFactory.getLogger(getClass());

        public PrivateStoreValidator() {
        }

        public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
            return logReadOnlyMountCommit(name, after);
        }

        public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
            return logReadOnlyMountCommit(name, after);
        }

        public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
            return logReadOnlyMountCommit(name, before);
        }

        private Validator logReadOnlyMountCommit(String nodeName, NodeState nodeState) throws CommitFailedException {
            Tree nodeStateTree = TreeFactory.createReadOnlyTree(nodeState);
            String nodePath = nodeStateTree.getPath();
            String changeNodePath = nodeStateTree.isRoot()
                    ? nodePath + nodeName
                    : nodePath + "/" + nodeName;

            Mount mountInfo = mountInfoProvider.getMountByPath(changeNodePath);
            if (mountInfo.isReadOnly()) {
                Throwable throwable = new Throwable("Commit path: " + changeNodePath);
                logger.error("Detected commit to a read-only store! ", throwable);

                if (failOnReadonlyMountCommit) {
                    throw new CommitFailedException(CommitFailedException.UNSUPPORTED, 0,
                            "Unsupported commit to a read-only store!", throwable);
                }
            }

            return null;
        }
    }
}
