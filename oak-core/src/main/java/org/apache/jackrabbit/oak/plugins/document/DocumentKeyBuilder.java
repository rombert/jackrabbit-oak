package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

public class DocumentKeyBuilder {

    public static DocumentKey fromKey(String key) {
        return new DocumentKeyImpl(key);        
    }
    
    public static DocumentKey fromPath(String path) {
        return new DocumentKeyImpl(Utils.getIdFromPath(path));
    }
}
