package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertTrue;

import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MultiplexingDocumentStoreWithNodeStoreTest {
    
    private ContentRepository repo;
    private MongoConnection connection;

    @Before
    public void createContentRepository() throws Exception{
        
        // 1. Connect to MongoDB
        connection = new MongoConnection("mongodb://localhost:27017/oak");
        dropMongoDatabase();
        
        // 2. configure the DocumentNodeStore with a multiplexing document store, with a mount at "/tmp"
        DocumentMK.Builder builder = new DocumentMK.Builder();
        builder.addMongoDbMount("/tmp", connection, "private");
        builder.setMongoDB(connection.getDB(), 1, 16);

        DocumentNodeStore store = new DocumentNodeStore(builder);

        // 3. Create the Oak instance
        repo = new Oak(store).with(new OpenSecurityProvider()).createContentRepository();        
    }
    
    @After
    public void dropMongoDatabase() {
        
        if ( connection != null ) {
            connection.getDB().dropDatabase();
        }
    }

    @Test
    public void mountedNodeCreationIsVisible() throws Exception {

        // 1. Create a tree at /tmp ( mounted store ) and one at /content ( root store )
        ContentSession session = repo.login(new SimpleCredentials("admin", "admin".toCharArray()), null);
        try {
            Root r = session.getLatestRoot();
            Tree root = r.getTree("/");
            root.addChild("tmp");
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
        } finally {
            session.close();
        }

    }

    private void assertTreeExists(Tree root, String childName) {
        
        Tree content = root.getChild(childName);
        assertTrue("Tree at " + content.getPath() + " does not exist", content.exists());
    }

}
