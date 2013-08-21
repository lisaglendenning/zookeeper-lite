package edu.uw.zookeeper.data;

import static org.junit.Assert.*;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.BasicOperationGenerator;
import edu.uw.zookeeper.client.PipeliningClient;
import edu.uw.zookeeper.client.SessionClientExecutor;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.LoggingPublisher;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.protocol.server.ZxidIncrementer;

@RunWith(JUnit4.class)
public class ZNodeDataTrieTest {
    
    protected final Logger logger = LogManager.getLogger();
    
    @Test(timeout=10000)
    public void testRandom() throws InterruptedException, ExecutionException {
        ZNodeDataTrieExecutor executor = ZNodeDataTrieExecutor.create(
                ZNodeDataTrie.newInstance(),
                ZxidIncrementer.fromZero(),
                LoggingPublisher.create(logger, EventBusPublisher.newInstance()));
        ZNodeViewCache<?, Records.Request, Message.ServerResponse<?>> cache = 
                ZNodeViewCache.newInstance(executor, SessionClientExecutor.create(1, executor));
        PipeliningClient<Records.Request, Message.ServerResponse<?>> operations = PipeliningClient.create(100, cache, BasicOperationGenerator.create(cache));
        List<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> futures = operations.next();
        for (Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> future: futures) {
            assertFalse(future.second().get().getRecord() instanceof Operation.Error);
        }
    }
}
