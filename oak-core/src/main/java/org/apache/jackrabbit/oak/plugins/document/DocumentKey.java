package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

/**
 * The <tt>DocumentKey</tt> wraps a string key value and allows easy retrieval
 * of the full value and the path as needed
 *
 */
public class DocumentKey {

    /**
     * @param key the key in the expanded representation, e.g. <em>1:/tmp</em>
     * @return the DocumentKey
     */
    public static DocumentKey fromKey(String key) {
        return new DocumentKey(key);
    }

    public static DocumentKey fromPath(String path) {
        return new DocumentKey(Utils.getIdFromPath(path));
    }

    private final String key;
    private final String path;

    private DocumentKey(String key) {

        this.key = key;
        this.path = Utils.getPathFromId(key);
    }

    public String getPath() {
        return path;
    }

    /**
     * Returns the raw key value
     * 
     * @return the raw key value, e.g. <em>1:/tmp</em>
     */
    public String getValue() {
        return key;
    }

}
