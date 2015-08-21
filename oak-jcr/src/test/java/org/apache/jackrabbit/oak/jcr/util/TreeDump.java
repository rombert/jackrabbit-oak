/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.util;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyIterator;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.Value;
import javax.jcr.util.TraversingItemVisitor;

/** Dump a JCR tree */
public class TreeDump {
    private final StringBuilder msg;
    
    public TreeDump(Session s, String rootPath) {
        msg = new StringBuilder();
        try {
            s.getNode(rootPath).accept(new TraversingItemVisitor.Default() {
                @Override
                protected void entering(Node node, int level)
                        throws RepositoryException {
                    if(!accept(node.getPath())) {
                        return;
                    }
                    String indent = "";
                    for (int i = 0; i < level; i++) {
                        indent += "  ";
                    }
                    msg.append(indent).append(node.getName()).append("\n");
                    PropertyIterator it = node.getProperties();
                    indent += "  ";
                    while (it.hasNext()) {
                        Property p = it.nextProperty();
                        msg.append(indent).append(p.getName()).append(": ");
                        if (p.isMultiple()) {
                            msg.append("[");
                            String sep = "";
                            Value[] values = p.getValues();
                            for (Value value : values) {
                                msg.append(sep);
                                msg.append(value.getString());
                                sep = ", ";
                            }
                            msg.append("]");
                        } else {
                            msg.append(p.getValue().getString());
                        }
                        msg.append("\n");
                    }
                }
            });
        } catch (RepositoryException e) {
            msg.append(e.toString());
        }
    }
    
    protected boolean accept(String path) {
        return true;
    }
    
    @Override
    public String toString() {
        return msg.toString();
    }
}
