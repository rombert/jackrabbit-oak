package org.apache.jackrabbit.oak.plugins.document;

public interface DocumentKey {

    String getPath();
    
    String getValue();
}
