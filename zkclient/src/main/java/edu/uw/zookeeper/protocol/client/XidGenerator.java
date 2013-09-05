package edu.uw.zookeeper.protocol.client;

import edu.uw.zookeeper.protocol.XidReference;

public interface XidGenerator extends XidReference {
    int next();
}
