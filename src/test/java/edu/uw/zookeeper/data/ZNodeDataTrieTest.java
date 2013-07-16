package edu.uw.zookeeper.data;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.TxnRequestProcessor;

@RunWith(JUnit4.class)
public class ZNodeDataTrieTest {

    @Test
    public void test() {
        ZNodeDataTrie trie = ZNodeDataTrie.newInstance();
    }
}
