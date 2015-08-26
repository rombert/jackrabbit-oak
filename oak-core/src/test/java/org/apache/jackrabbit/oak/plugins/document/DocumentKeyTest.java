package org.apache.jackrabbit.oak.plugins.document;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class DocumentKeyTest {
    
    // TODO - generate the values instead of hardcoding, limits might change, see LongPathTest
    private static final String LONG_PATH = "/n0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_0/n0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_1/n0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_2/n0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_3/n0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_4";
    private static final String HASHED_ID = "5:h516d8be3b20711bf40e59696aa081565c81b27b8cf469a2906dcd5c886ebf1dd/n0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789_4";
    
    @Test
    public void fromHashedKey() {
        DocumentKey key = DocumentKey.fromKey(HASHED_ID);
        assertNull(key.getPath());
        assertThat(key.getValue(), is(HASHED_ID));
    }
    
    @Test
    public void fromKey() {
        
        DocumentKey key = DocumentKey.fromKey("0:/");
        assertThat(key.getPath(), is("/"));
        assertThat(key.getValue(), is("0:/"));
    }

    @Test
    public void fromPath() {
        
        DocumentKey key = DocumentKey.fromPath("/");
        assertThat(key.getPath(), is("/"));
        assertThat(key.getValue(), is("0:/"));
    }

    @Test
    public void fromLongPath() {
        
        assertThat(DocumentKey.fromPath(LONG_PATH).getPath(), is(LONG_PATH));
    }
}