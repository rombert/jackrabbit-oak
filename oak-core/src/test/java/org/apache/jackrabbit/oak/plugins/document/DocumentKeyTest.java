package org.apache.jackrabbit.oak.plugins.document;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class DocumentKeyTest {
    
    @Test
    public void fromKey() {
        
        assertThat(DocumentKey.fromKey("0:/").getPath(), is("/"));
    }

    @Test
    public void fromPath() {
        
        assertThat(DocumentKey.fromPath("/").getPath(), is("/"));
    }
}
