package org.apache.jackrabbit.oak.jcr.lock;

import static org.junit.Assert.assertTrue;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import javax.jcr.LoginException;
import javax.jcr.Node;
import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.plugins.commit.JcrConflictHandler;
import org.apache.jackrabbit.oak.plugins.document.DocumentMK;
import org.apache.jackrabbit.oak.plugins.document.DocumentNodeStore;
import org.apache.jackrabbit.oak.plugins.document.util.MongoConnection;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.google.common.collect.Maps;
import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoClientURI;

@RunWith(Parameterized.class)
public class MultiplexingNodeStoreCurrentFailuresTest {
    
    @Parameters(name = "multiplexing: {0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] { 
            { false} , { true}
        });
    }

    private final boolean useMultiplexing;
    
    public MultiplexingNodeStoreCurrentFailuresTest(boolean useMultiplexing) {
        this.useMultiplexing = useMultiplexing;
    }

    private Repository repo;
    private DocumentNodeStore nodeStore;

    @Before
    public void prepare() throws Exception {

        initRepo(true);
        
        Session session = getAdminSession();

        Node first = session.getRootNode().addNode("first");
        first.addMixin("mix:lockable");
        session.save();
        session.logout();
    }
    
    private Session getAdminSession() throws LoginException, RepositoryException {
        return repo.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }
        

    private void initRepo(boolean dropDatabase) throws UnknownHostException {
        String db = "oak-test-mpx-" + useMultiplexing;
        String uri = "mongodb://localhost:27017/" + db;
        
        DocumentMK.Builder mkBuilder = new DocumentMK.Builder();

        MongoClientOptions.Builder builder = MongoConnection.getDefaultBuilder();
        MongoClientURI mongoURI = new MongoClientURI(uri, builder);

        MongoClient client = new MongoClient(mongoURI);
        DB mongoDB = client.getDB(db);

        if ( dropDatabase) {
            mongoDB.dropDatabase();
        }

        if ( useMultiplexing ) {
            Map<String, String> mounts = Maps.newLinkedHashMap();
            mounts.put("/extra", "extra");
            
            for (Map.Entry<String, String> entry : mounts.entrySet()) {
                mkBuilder.addMongoDbMount(entry.getKey(), mongoDB, entry.getValue());
            }
        }

        mkBuilder.setMongoDB(mongoDB, 256, 16);

        nodeStore = mkBuilder.open().getNodeStore();
        
        Jcr jcr = new Jcr(nodeStore).
                with(new InitialContent()).
                with(new Indexes()).
                with(JcrConflictHandler.createJcrConflictHandler());
        
        repo = jcr.createRepository();
    }

    @Test
    @Ignore("OAK-3152")
    public void loopLocking() throws Exception {
        
        int locksPerIteration = 10;
        int iterations = 10;

        for (int i = 0; i < iterations; i++) {

            for (int j = 0; j < locksPerIteration; j++) {

                Session session = getAdminSession();

                try {

                    Node first = session.getNode("/first");

                    first.lock(true, false);
                    first.unlock();
                } finally {
                    session.logout();
                }
            }

            reinitRepo(0);
        }

    }

    @Test
    public void contentWrittenUnderMountIsNoLongerVisible() throws Exception {

        // 1. create node exactly at the mount point
        {
            Session session = getAdminSession();
            try {
                session.getRootNode().addNode("extra");
                session.save();
            } finally {
                session.logout();
            }
        }
        
        // 2. validate that the created node is visible
        {
            Session session = getAdminSession();
            try {
                assertTrue("Node at '/extra' not found", session.getRootNode().hasNode("extra"));
            } finally {
                session.logout();
            }
        }
        
        // 3. create node under the newly created one
        {
            Session session = getAdminSession();
            try {
                session.getRootNode().getNode("extra").
                    addNode("kiddo");
                session.save();
            } finally {
                session.logout();
            }
        }
        
        // 4. validate that the created node is visible
        {
            Session session = getAdminSession();
            try {
                Node parent = session.getRootNode().
                        getNode("extra");
                assertTrue("Node at '/extra/kiddo' not found", parent.hasNode("kiddo"));
            } finally {
                session.logout();
            }
        }
        
    }
   
    /**
     * Reinitialises the repository instance, optionally waiting for <tt>wait</tt> milliseconds
     * 
     * @param wait the number of millis to wait, ignored if <= 0
     * @throws InterruptedException
     * @throws UnknownHostException
     */
    private void reinitRepo(long wait) throws InterruptedException, UnknownHostException {
        ((JackrabbitRepository) repo).shutdown();
        if ( wait > 0 ) {
            Thread.sleep(wait);
        }
        initRepo(false);
    }
    
    static class Indexes implements RepositoryInitializer {

        @Override
        public void initialize(NodeBuilder root) {
            
            if ( !root.hasChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)) {
                return;
            }
            
            NodeBuilder index = root.child(IndexConstants.INDEX_DEFINITIONS_NAME);

            if (!index.hasChildNode("jcrLockOwner")) {
                IndexUtils.createIndexDefinition(index, "jcrLockOwner", true, false, Collections.singleton("jcr:lockOwner"), null);
            }

        }        
    }
}