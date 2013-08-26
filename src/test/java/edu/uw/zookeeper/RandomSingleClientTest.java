package edu.uw.zookeeper;

import static org.junit.Assert.assertFalse;

import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.BasicRequestGenerator;
import edu.uw.zookeeper.client.CallUntilPresent;
import edu.uw.zookeeper.client.IterationCallable;
import edu.uw.zookeeper.client.SimpleClient;
import edu.uw.zookeeper.client.SubmitCallable;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.Generator;
import edu.uw.zookeeper.common.ListAccumulator;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class RandomSingleClientTest {

    @Test(timeout=10000)
    public void testRandom() throws Exception {
        SimpleClient client = SimpleClient.newInstance();

        client.startAsync();
        client.awaitRunning();
        
        ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache = 
                ZNodeViewCache.newInstance(client.getClient(), client.getClient());
        int iterations = 100;
        Generator<Records.Request> requests = BasicRequestGenerator.create(cache);
        ListAccumulator<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> accumulator = ListAccumulator.create(
                SubmitCallable.create(requests, cache),
                Lists.<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>>newArrayListWithCapacity(iterations)); 
        List<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> results = 
                CallUntilPresent.create(IterationCallable.create(iterations, accumulator)).call();
        for (Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> result: results) {
            assertFalse(result.second().get().record() instanceof Operation.Error);
        }

        client.stopAsync();
        client.awaitTerminated();
    }
}
