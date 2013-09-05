package edu.uw.zookeeper.protocol.server;

import edu.uw.zookeeper.protocol.ZxidReference;

public interface ZxidGenerator extends ZxidReference {
    long next();
}
