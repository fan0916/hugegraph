/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.dist;

import java.io.FileDescriptor;
import java.security.Permission;

import com.baidu.hugegraph.HugeException;

public class HugeSecurityManager extends SecurityManager {

    private static final String GremlinExecutor_Class =
            "org.codehaus.groovy.runtime.callsite.AbstractCallSite";

    @Override
    public void checkPermission(Permission permission) {
        // allow anything.
    }

    @Override
    public void checkPermission(Permission permission, Object context) {
        // allow anything.
    }

    @Override
    public void checkAccess(ThreadGroup g) {
        if (this.callFromGremlin()) {
            throw new HugeException("Not allowed to modify thread via gremlin");
        } else {
            super.checkAccess(g);
        }
    }

    @Override
    public void checkExit(int status) {
        if (this.callFromGremlin()) {
            throw new HugeException("Not allowed to call System.exit() via gremlin");
        } else {
            super.checkExit(status);
        }
    }

    @Override
    public void checkRead(FileDescriptor fd) {
        if (this.callFromGremlin()) {
            throw new HugeException("Not allowed to read file via gremlin");
        } else {
            super.checkRead(fd);
        }
    }

//    @Override
//    public void checkRead(String file) {
//        if (this.callFromGremlin()) {
//            throw new HugeException("Not allowed to read file via gremlin");
//        } else {
//            super.checkRead(file);
//        }
//    }
//
//    @Override
//    public void checkRead(String file, Object context) {
//        if (this.callFromGremlin()) {
//            throw new HugeException("Not allowed to read file via gremlin");
//        } else {
//            super.checkRead(file, context);
//        }
//    }

    @Override
    public void checkWrite(FileDescriptor fd) {
        if (this.callFromGremlin()) {
            throw new HugeException("Not allowed to write file via gremlin");
        } else {
            super.checkWrite(fd);
        }
    }

    @Override
    public void checkWrite(String file) {
        if (this.callFromGremlin()) {
            throw new HugeException("Not allowed to write file via gremlin");
        } else {
            super.checkWrite(file);
        }
    }

    @Override
    public void checkAccept(String host, int port) {
        if (this.callFromGremlin()) {
            throw new HugeException("Not allowed to accept connect via gremlin");
        } else {
            super.checkAccept(host, port);
        }
    }

    @Override
    public void checkConnect(String host, int port) {
        if (this.callFromGremlin()) {
            throw new HugeException("Not allowed to connect socket via gremlin");
        } else {
            super.checkConnect(host, port);
        }
    }

    private boolean callFromGremlin() {
        StackTraceElement elements[] = Thread.currentThread().getStackTrace();
        for (StackTraceElement element : elements) {
            String className = element.getClassName();
            if (GremlinExecutor_Class.equals(className)) {
                return true;
            }
        }
        return false;
    }
}
