package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertTrue;

import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultiplexingDocumentStoreWithNodeStoreTest {
    
    private ContentRepository repo;
    private DocumentNodeStore store;

    @Before
    public void createContentRepository() throws Exception{
        
        MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                .mount("tmp", "/tmp")
                .build();
        
        // 1. configure the DocumentNodeStore with a multiplexing document store, with a mount at "/tmp"
        DocumentMK.Builder builder = new DocumentMK.Builder();
        builder.setMountInfoProvider(mip);
        builder.addMemoryMount("tmp");

        store = new DocumentNodeStore(builder);

        // 2. Create the Oak instance
        repo = new Oak(store).with(new OpenSecurityProvider()).createContentRepository();        
    }
    
    @After
    public void cleanup() {
        if ( store != null ) {
            store.dispose();
        }
    }

    @Test
    public void mountedNodeCreationIsVisible() throws Exception {

        // 1. Create a tree at /tmp ( mounted store ) and one at /content ( root store )
        ContentSession session = repo.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        try {
            Root r = session.getLatestRoot();
            Tree root = r.getTree("/");
            Tree tmp = root.addChild("tmp");
            tmp.addChild("child");
            root.addChild("content");
            r.commit();
        } finally {
            session.close();
        }
        
        // 2. validate that the previously created trees are now visible
        session = repo.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        try {
            Tree root = session.getLatestRoot().getTree("/");
            assertTreeExists(root, "content");
            assertTreeExists(root, "tmp");
            Tree tmp = root.getChild("tmp");
            assertTreeExists(tmp, "child");
        } finally {
            session.close();
        }

    }

    private void assertTreeExists(Tree root, String childName) {
        
        Tree content = root.getChild(childName);
        assertTrue("Tree at " + content.getPath() + " does not exist", content.exists());
    }
    
    @Test
    public void splitRevisions() throws Exception {

        MultiplexingBasedDocumentSplitTest delegate = new MultiplexingBasedDocumentSplitTest();
        delegate.initDocumentMK(); // not managed by JUnit so call the @Before manually
        delegate.splitRevisions();
    }

    /**
     * Variant of the <tt>DocumentSplitTest</tt> which uses a multiplexed Documentstore
     *
     */
    private static final class MultiplexingBasedDocumentSplitTest extends DocumentSplitTest {
        
        @Override
        public void initDocumentMK() {
            MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                    .mount("extra", "/extra")
                    .build();
            
            DocumentMK.Builder mkBuilder = new DocumentMK.Builder();
            mkBuilder.setMountInfoProvider(mip);

            mk = mkBuilder.open();
        }
    }
}
