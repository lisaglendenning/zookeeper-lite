package edu.uw.zookeeper.protocol.server;

public interface ZxidGenerator extends ZxidReference {
    long next();
}
