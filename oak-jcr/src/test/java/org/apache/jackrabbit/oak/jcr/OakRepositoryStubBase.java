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
package org.apache.jackrabbit.oak.jcr;

import java.io.File;
import java.io.IOException;
import java.security.Principal;
import java.util.Iterator;
import java.util.Properties;

import javax.jcr.Credentials;
import javax.jcr.GuestCredentials;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.UnsupportedRepositoryOperationException;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.test.RepositoryStub;

public class OakRepositoryStubBase extends RepositoryStub {

    private static final int MAX_FILE_SIZE = 64 * 1024 * 1024;
    private static final int CACHE_SIZE = 32 * 1024 * 1024;
    private static final boolean MMAP =
            System.getProperty("sun.arch.data.model", "32").equals("64");

    private final Repository repository;

    /**
     * Constructor as required by the JCR TCK.
     * 
     * @param settings repository settings
     */
    public OakRepositoryStubBase(Properties settings)
            throws RepositoryException {
        super(settings);

        try {
            File dir = new File("target", "mk-tck-" + System.currentTimeMillis());
            Jcr jcr = new Jcr(new SegmentNodeStore(
                    new FileStore(dir, MAX_FILE_SIZE, CACHE_SIZE, MMAP)));
            preCreateRepository(jcr);
            repository = jcr.createRepository();
        } catch (IOException e) {
            throw new RepositoryException(e);
        }
    }

    protected void preCreateRepository(Jcr jcr) {
    }

    /**
     * Returns the configured repository instance.
     * 
     * @return the configured repository instance.
     */
    @Override
    public synchronized Repository getRepository() {
        return repository;
    }

    @Override
    public Credentials getReadOnlyCredentials() {
        return new GuestCredentials();
    }

    @Override
    public Principal getKnownPrincipal(Session session) throws RepositoryException {
        if (session instanceof JackrabbitSession) {
            Iterator<Principal> principals = ((JackrabbitSession) session).getPrincipalManager().getPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
            if (principals.hasNext()) {
                return principals.next();
            }
        }

        throw new UnsupportedRepositoryOperationException();
    }

    private static final Principal UNKNOWN_PRINCIPAL = new Principal() {
        @Override
        public String getName() {
            return "an_unknown_user";
        }
    };

    @Override
    public Principal getUnknownPrincipal(Session session) throws RepositoryException, NotExecutableException {
        return UNKNOWN_PRINCIPAL;
    }

}
