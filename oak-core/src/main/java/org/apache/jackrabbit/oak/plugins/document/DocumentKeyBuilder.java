package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.plugins.document.util.Utils;

public class DocumentKeyBuilder {

    public static DocumentKey of(String path) {
        return new DocumentKeyImpl(Utils.getIdFromPath(path));
    }
}
