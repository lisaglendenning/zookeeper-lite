package edu.uw.zookeeper.data;

import static org.junit.Assert.*;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.BasicRequestGenerator;
import edu.uw.zookeeper.client.CallUntilPresent;
import edu.uw.zookeeper.client.IterationCallable;
import edu.uw.zookeeper.client.SessionClientExecutor;
import edu.uw.zookeeper.client.SubmitCallable;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.EventBusPublisher;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.ListAccumulator;
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
    public void testRandom() throws Exception {
        ZNodeDataTrieExecutor executor = ZNodeDataTrieExecutor.create(
                ZNodeDataTrie.newInstance(),
                ZxidIncrementer.fromZero(),
                LoggingPublisher.create(logger, EventBusPublisher.newInstance()));
        ZNodeViewCache<?, Records.Request, Message.ServerResponse<?>> cache = 
                ZNodeViewCache.newInstance(executor, SessionClientExecutor.create(1, executor));
        int iterations = 100;
        Generator<Records.Request> requests = BasicRequestGenerator.create(cache);
        ListAccumulator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> accumulator = ListAccumulator.create(
                SubmitCallable.create(requests, cache),
                Lists.<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>>newArrayListWithCapacity(iterations)); 
        List<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> results = 
                CallUntilPresent.create(IterationCallable.create(iterations, accumulator)).call();
        for (Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> result: results) {
            assertFalse(result.second().get().getRecord() instanceof Operation.Error);
        }
    }
}
