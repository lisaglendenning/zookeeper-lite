package edu.uw.zookeeper.data;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.ImmutableMap;

import edu.uw.zookeeper.client.SessionClientProcessor;
import edu.uw.zookeeper.data.ZNodeDataTrie.Operators;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.ByOpcodeTxnRequestProcessor;
import edu.uw.zookeeper.protocol.server.ToTxnRequestProcessor;

@RunWith(JUnit4.class)
public class ZNodeDataTrieTest {

    @Test
    public void test() throws KeeperException {
        ZNodeDataTrie trie = ZNodeDataTrie.newInstance();
        ByOpcodeTxnRequestProcessor operator = ByOpcodeTxnRequestProcessor.create(ImmutableMap.copyOf(Operators.of(trie)));
        SessionClientProcessor sessionProcessor = SessionClientProcessor.create(1);
        ToTxnRequestProcessor txnProcessor = ToTxnRequestProcessor.create();
        ZNodeLabel.Path path = ZNodeLabel.Path.of("/foo");
        Records.Response response = operator.apply(txnProcessor.apply(sessionProcessor.apply(Operations.Requests.create().setPath(path).build())));
        response = operator.apply(txnProcessor.apply(sessionProcessor.apply(Operations.Requests.exists().setPath(path).build())));
        response = operator.apply(txnProcessor.apply(sessionProcessor.apply(Operations.Requests.delete().setPath(path).build())));
    }
}
