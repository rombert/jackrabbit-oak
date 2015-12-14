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

package org.apache.jackrabbit.oak.security.authorization.permission;

import java.util.Collection;
import java.util.List;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.RestrictionProvider;

class MultiplexingPermissionStore implements PermissionStore, PermissionConstants {
    private final List<PermissionStoreImpl> stores;

    public MultiplexingPermissionStore(Root root, String workspaceName,
                                  RestrictionProvider restrictionProvider,
                                  MountInfoProvider mountInfoProvider){
        stores = Lists.newArrayListWithCapacity(1 + mountInfoProvider.getNonDefaultMounts().size());
        stores.add(new PermissionStoreImpl(root, workspaceName, restrictionProvider, PermissionTreeProvider.DEFAULT));
        for (Mount m : mountInfoProvider.getNonDefaultMounts()){
            stores.add(new PermissionStoreImpl(root, workspaceName,
                    restrictionProvider, new MountedPermTreeProvider(m)));
        }
    }


    @Override
    public Collection<PermissionEntry> load(@Nullable Collection<PermissionEntry> entries,
                                            @Nonnull String principalName, @Nonnull String path) {
        Collection<PermissionEntry> result = null;
        for (PermissionStoreImpl store : stores){
            result = store.load(result, principalName, path);
        }
        return result;
    }

    @Nonnull
    @Override
    public PrincipalPermissionEntries load(@Nonnull String principalName) {
        PrincipalPermissionEntries ret = new PrincipalPermissionEntries();
        for (PermissionStoreImpl store : stores){
            ret.getEntries().putAll(store.load(principalName).getEntries());
        }
        ret.setFullyLoaded(true);
        return ret;
    }

    @Override
    public long getNumEntries(@Nonnull String principalName, long max) {
        long ret = 0;
        //TODO [multiplex] Honour max
        for (PermissionStoreImpl store : stores){
            ret += store.getNumEntries(principalName, max);
        }
        return ret;
    }

    public void flush(Root root){
        for (PermissionStoreImpl store : stores){
            store.flush(root);
        }
    }

    private static class MountedPermTreeProvider implements PermissionTreeProvider {
        private final Mount mount;

        public MountedPermTreeProvider(Mount mount) {
            this.mount = mount;
        }

        @Override
        public Tree getPermissionTree(Root root, String workspaceName) {
            return root.getTree(PathUtils.concat(PERMISSIONS_STORE_PATH, mount.getPathFragmentName(), workspaceName));
        }
    }
}
