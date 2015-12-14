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

import java.security.Principal;
import java.util.Collections;

import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.Mount;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MultiplexingSupportTest extends AbstractSecurityTest implements AccessControlConstants,
        PrivilegeConstants, PermissionConstants {
    private MountInfoProvider mountInfoProvider;
    private String testPath = "/testPath";

    @Override
    @Before
    public void before() throws Exception {
        mountInfoProvider = SimpleMountInfoProvider.newBuilder()
                .mount("testMount", testPath + "/a")
                .build();

        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), namePathMapper);
        NodeUtil testNode = rootNode.addChild("testPath", JcrConstants.NT_UNSTRUCTURED);
        testNode.addChild("childNode", JcrConstants.NT_UNSTRUCTURED);
        root.commit();
    }

    @Override
    @After
    public void after() throws Exception {
        try {
            root.refresh();
            Tree test = root.getTree(testPath);
            if (test.exists()) {
                test.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters authConfig = ConfigurationParameters.of(
                Collections.singletonMap(AccessControlConstants.PARAM_MOUNT_PROVIDER,
                        Preconditions.checkNotNull(mountInfoProvider)));
        return ConfigurationParameters.of(ImmutableMap.of(
                AuthorizationConfiguration.NAME, authConfig));
    }

    @Test
    public void multiplexingPermissionStore() throws Exception {
        String path_a = testPath + "/a";
        String path_c = testPath + "/c";
        String path_d = testPath + "/d";

        Principal p = getTestUser().getPrincipal();
        NodeUtil testNode = new NodeUtil(root.getTree(testPath), namePathMapper);
        testNode.addChild("a", JcrConstants.NT_UNSTRUCTURED);
        testNode.addChild("c", JcrConstants.NT_UNSTRUCTURED);
        testNode.addChild("d", JcrConstants.NT_UNSTRUCTURED);

        setPrivs(path_a, privilegesFromNames(JCR_READ, REP_WRITE), p);
        setPrivs(path_c, privilegesFromNames(JCR_READ, REP_WRITE), p);

        root.commit();

        assertTrue(root.getTree(path_a + "/rep:policy").exists());
        assertTrue(root.getTree(path_c + "/rep:policy").exists());

        Tree principalRoot = getPrincipalRoot(mountInfoProvider, path_a, p.getName());
        assertEquals(2, cntEntries(principalRoot));

        principalRoot = getPrincipalRoot(mountInfoProvider, path_c, p.getName());
        assertEquals(2, cntEntries(principalRoot));

        ContentSession testSession = createTestSession();

        try {
            Root r = testSession.getLatestRoot();
            assertTrue(r.getTree(path_a).exists());
            assertTrue(r.getTree(path_c).exists());
            assertFalse(r.getTree(path_d).exists());
        }finally {
            testSession.close();
        }
    }

    private void setPrivs(String path, Privilege[] privileges, Principal principal) throws Exception {
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, path);
        acl.addAccessControlEntry(principal, privileges);
        acMgr.setPolicy(path, acl);
    }

    private Tree getPrincipalRoot(MountInfoProvider mip, String path, String principalName) {
        Mount m = mip.getMountByPath(path);
        Tree permStore = root.getTree(PERMISSIONS_STORE_PATH);
        if (!m.isDefault()) {
            permStore = root.getTree(PERMISSIONS_STORE_PATH).getChild(m.getPathFragmentName());
        }
        return permStore.getChild(adminSession.getWorkspaceName()).getChild(principalName);
    }

    protected long cntEntries(Tree parent) {
        long cnt = parent.getChildrenCount(Long.MAX_VALUE);
        for (Tree child : parent.getChildren()) {
            cnt += cntEntries(child);
        }
        return cnt;
    }
}
