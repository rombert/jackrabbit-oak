package org.apache.jackrabbit.oak.jcr.lock;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import javax.jcr.Node;
import javax.jcr.Session;
import javax.jcr.lock.Lock;
import javax.jcr.lock.LockException;

import org.apache.jackrabbit.oak.jcr.AbstractRepositoryTest;
import org.apache.jackrabbit.oak.jcr.NodeStoreFixture;
import org.junit.Test;

public class OpenScopedLocksTest extends AbstractRepositoryTest {

    public OpenScopedLocksTest(NodeStoreFixture fixture) {
        super(fixture);
    }

    @Test
    public void openScopedLockIsReflectedInAnotherSession() throws Exception {
        Session session = createAdminSession();
        try {
            // make versionable
            Node node = session.getRootNode().addNode("child1");
            node.addMixin("mix:lockable");
            node.getSession().save();

            // lock
            session.getWorkspace().getLockManager().lock(node.getPath(), true, false, Long.MAX_VALUE, null);
        } finally {
            session.logout();
        }
        
        Node node = createAdminSession().getNode("/child1");
        
        assertTrue(node.isLocked());
        
        Lock lock = node.getSession().getWorkspace().getLockManager().getLock(node.getPath());
        
        assertFalse(lock.isLockOwningSession());
        
        try {
            node.setProperty("some", "value");
            node.getSession().save();
            fail("Setting a property on a locked node should have thrown a LockException");
        } catch ( LockException e ) {
            // I'll allow it
        } finally {
            node.getSession().logout();
        }
        
    }

}
