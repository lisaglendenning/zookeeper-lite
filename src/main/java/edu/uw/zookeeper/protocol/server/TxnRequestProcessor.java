package edu.uw.zookeeper.protocol.server;

import org.apache.zookeeper.KeeperException;

import edu.uw.zookeeper.data.TxnOperation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processors;

public interface TxnRequestProcessor<T extends Records.Request, V extends Records.Response> extends Processors.CheckedProcessor<TxnOperation.Request<T>, V, KeeperException> {
}
