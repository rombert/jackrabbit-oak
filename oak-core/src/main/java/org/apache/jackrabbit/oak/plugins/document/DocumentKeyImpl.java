package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

public class DocumentKeyImpl implements DocumentKey {

    public static DocumentKey fromKey(String key) {
        return new DocumentKeyImpl(key);        
    }
    
    public static DocumentKey fromPath(String path) {
        return new DocumentKeyImpl(Utils.getIdFromPath(path));
    }
    
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
