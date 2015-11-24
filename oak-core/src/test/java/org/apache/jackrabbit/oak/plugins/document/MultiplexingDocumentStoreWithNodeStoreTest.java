package org.apache.jackrabbit.oak.plugins.document;

import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.Map;

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

import com.google.common.collect.Maps;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;

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
        builder.addMongoDbMount("/tmp", "mongodb://localhost:27017/oak", "oak", "private");
        builder.setMongoDB(connection.getDB(), 1);

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
            try {
                boolean useMultiplexing = true;
                boolean dropDatabase = true;
                
                String db = "oak-test-mpx-" + useMultiplexing;
                String uri = "mongodb://localhost:27017/" + db;
                
                DocumentMK.Builder mkBuilder = new DocumentMK.Builder();

                MongoClientOptions.Builder builder = MongoConnection.getDefaultBuilder();
                MongoClientURI mongoURI = new MongoClientURI(uri, builder);

                MongoClient client;
                try {
                    client = new MongoClient(mongoURI);
                } catch (UnknownHostException e) {
                    throw new RuntimeException(e);
                }
                DB mongoDB = client.getDB(db);

                if ( dropDatabase) {
                    mongoDB.dropDatabase();
                }

                if ( useMultiplexing ) {
                    Map<String, String> mounts = Maps.newLinkedHashMap();
                    mounts.put("/extra", "extra");
                    
                    for (Map.Entry<String, String> entry : mounts.entrySet()) {
                        mkBuilder.addMongoDbMount(entry.getKey(), uri, db, entry.getValue());
                    }
                }

                mkBuilder.setMongoDB(uri, db, 256);

                mk = mkBuilder.open();
            } catch (UnknownHostException e) {
                throw new RuntimeException(e);
            }                
        }
    }
}
