package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

public class DocumentKeyImpl implements DocumentKey {

    private final String key;
    private final String path;
    
    public DocumentKeyImpl(String key) {
        
        this.key = key;
        this.path = Utils.getPathFromId(key);
    }
    
    @Override
    public String getPath() {
        return path;
    }
    
    public String getValue() {
        return key;
    }

}
