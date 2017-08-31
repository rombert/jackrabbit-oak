/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.composite.checks;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Utility class for dumping contents of nodes
 *
 */
public abstract class RepositoryDumper {

    /**
     * Recursively prints on <tt>System.out</tt> this node and its children
     * 
     * @param root the first node to dump
     */
    public static void dump(NodeState root) {
        System.out.println("--------");
        dump0(root, 0);
        System.out.println("--------");
    }

    private static void dump0(NodeState node, int indent) {
        for ( PropertyState prop: node.getProperties() ) {
            for (int i = 0 ; i < indent; i++ ) 
                System.out.print(' ');
            
            String val = prop.isArray() ? prop.getValue(Type.STRINGS).toString() : prop.getValue(Type.STRING);
            
            System.out.format("+ %s: %s%n", prop.getName(), val);
        }
        
        for ( ChildNodeEntry child : node.getChildNodeEntries() ) {
            for (int i = 0 ; i < indent; i++ ) 
                System.out.print(' ');
            System.out.format("- %s%n", child.getName());
            
            dump0(child.getNodeState(), indent + 2);
        }
    }
    
    private RepositoryDumper() {
        
    }
}
