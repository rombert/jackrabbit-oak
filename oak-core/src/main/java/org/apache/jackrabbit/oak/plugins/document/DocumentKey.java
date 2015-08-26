package org.apache.jackrabbit.oak.plugins.document;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

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
    public static DocumentKey fromKey(@Nonnull String key) {
        if ( Utils.isIdFromLongPath(key)) {
            return new DocumentKey(key, null);
        }
        return new DocumentKey(key, Utils.getPathFromId(key));
    }

    public static DocumentKey fromPath(@Nonnull String path) {
        return new DocumentKey(Utils.getIdFromPath(path), path);
    }

    private final String key;
    private final String path;

    private DocumentKey(String key, String path) {

        this.path = path;
        this.key = key;
        
    }
    
    public @Nullable String getPath() {
        return path;
    }

    /**
     * Returns the raw key value
     * 
     * @return the raw key value, e.g. <em>1:/tmp</em>
     */
    public @Nonnull String getValue() {
        return key;
    }
}
