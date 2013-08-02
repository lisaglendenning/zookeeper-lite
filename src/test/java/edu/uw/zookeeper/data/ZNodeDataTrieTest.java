package edu.uw.zookeeper.data;

import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;


import edu.uw.zookeeper.client.RandomCacheOperationClient;
import edu.uw.zookeeper.client.SessionClientExecutor;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.SessionOperation;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class ZNodeDataTrieTest {
    
    protected final Logger logger = LogManager.getLogger();
    
    @Test(timeout=10000)
    public void testRandom() throws InterruptedException, ExecutionException {
        ZNodeDataTrieExecutor executor = ZNodeDataTrieExecutor.create();
        ZNodeViewCache<?, Records.Request, SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> cache = 
                ZNodeViewCache.newInstance(executor, SessionClientExecutor.create(1, executor));
        RandomCacheOperationClient<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> client = RandomCacheOperationClient.create(cache);
        int noperations = 100;
        for (int i=0; i<noperations; ++i) {
            Pair<SessionOperation.Request<Records.Request>, Message.ServerResponse<Records.Response>> operation = client.call().get();
            logger.debug("{}", operation);
        }
    }
}
