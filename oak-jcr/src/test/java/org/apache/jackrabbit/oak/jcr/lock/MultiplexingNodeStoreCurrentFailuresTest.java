package org.apache.jackrabbit.oak.jcr.lock;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
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
import org.apache.jackrabbit.oak.plugins.multiplex.SimpleMountInfoProvider;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
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
    
    @After
    public void cleanup() throws Exception {
        if ( repo instanceof JackrabbitRepository ) {
            ((JackrabbitRepository) repo).shutdown();
        }
        
        nodeStore.dispose();
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
            MountInfoProvider mip = SimpleMountInfoProvider.newBuilder()
                    .mount("extra", "/extra")
                    .build();
            mkBuilder.setMountInfoProvider(mip);
            Map<String, String> mounts = Maps.newLinkedHashMap();
            mounts.put("extra", "extra");
            
            for (Map.Entry<String, String> entry : mounts.entrySet()) {
                mkBuilder.addMongoDbMount(entry.getKey(), uri, db, entry.getValue());
            }
        }

        mkBuilder.setMongoDB(mongoDB, 256);

        nodeStore = mkBuilder.open().getNodeStore();
        
        Jcr jcr = new Jcr(nodeStore).
                with(new InitialContent()).
                with(new Indexes()).
                with(JcrConflictHandler.createJcrConflictHandler());
        
        repo = jcr.createRepository();
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
    
    @Test
    public void recreateFileUnderMountPath() throws Exception {
        // 1. create a folder
        {
            Node extra = getAdminSession().getRootNode().addNode("extra", "nt:folder");
            extra.addNode("folder", "nt:folder");
            extra.getSession().save();
            extra.getSession().logout();
        }

        // 2. add a file
        {
            Node folder = getAdminSession().getNode("/extra/folder");
            Node file = folder.addNode("first.txt", "nt:file");
            Node contentNode = file.addNode("jcr:content","nt:resource");
            contentNode.setProperty("jcr:data", new ByteArrayInputStream("hello, world".getBytes()));
            
            folder.getSession().save();
            folder.getSession().logout();
        }
        
        // 3. delete the file
        {
            Node file = getAdminSession().getNode("/extra/folder/first.txt"); 
            file.remove();
            file.getSession().save();
            file.getSession().logout();
        }
        
        // 4. re-add the file
        {
            Node folder = getAdminSession().getNode("/extra/folder");
            Node file = folder.addNode("first.txt", "nt:file");
            Node contentNode = file.addNode("jcr:content","nt:resource");
            contentNode.setProperty("jcr:data", new ByteArrayInputStream("hello, world".getBytes()));
            
            folder.getSession().save();
            folder.getSession().logout();
        }
        
        // 5. verify that the file exists
        {
            Node file = getAdminSession().getNode("/extra/folder/first.txt");
            assertEquals("nt:file", file.getPrimaryNodeType().getName());
        }
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