package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.CheckForNull;

public interface HierarchicalDocumentStore extends DocumentStore {
    
    @CheckForNull
    <T extends Document> T findNode(DocumentKey key);
    
    @CheckForNull
    <T extends Document> T findNode(DocumentKey key, int maxCacheAge);
}
