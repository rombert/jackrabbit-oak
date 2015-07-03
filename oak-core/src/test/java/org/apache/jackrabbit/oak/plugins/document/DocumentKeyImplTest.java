package org.apache.jackrabbit.oak.plugins.document;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.Test;

public class DocumentKeyImplTest {
    
    @Test
    public void fromKey() {
        
        assertThat(DocumentKeyImpl.fromKey("0:/").getPath(), is("/"));
    }

    @Test
    public void fromPath() {
        
        assertThat(DocumentKeyImpl.fromPath("/").getPath(), is("/"));
    }
}
