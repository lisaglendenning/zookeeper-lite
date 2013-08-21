package edu.uw.zookeeper.protocol.client;

public interface XidGenerator extends XidReference {
    int next();
}
