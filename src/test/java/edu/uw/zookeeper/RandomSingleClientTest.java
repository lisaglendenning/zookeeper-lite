package edu.uw.zookeeper;

import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.ExecutionException;
import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.RandomCacheOperationClient;
import edu.uw.zookeeper.client.SimpleClient;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;

@RunWith(JUnit4.class)
public class RandomSingleClientTest {

    @Test(timeout=10000)
    public void testRandom() throws InterruptedException, ExecutionException, KeeperException {
        SimpleClient client = SimpleClient.newInstance();

        client.start().get();
        
        ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache = 
                ZNodeViewCache.newInstance(client.getClient(), client.getClient());
        RandomCacheOperationClient<Message.ServerResponse<?>> random = RandomCacheOperationClient.create(cache);
        
        int noperations = 100;
        List<ListenableFuture<Message.ServerResponse<?>>> futures = Lists.newArrayListWithCapacity(noperations);
        for (int i=0; i<noperations; ++i) {
            futures.add(random.call());
        }
        List<Message.ServerResponse<?>> responses = Futures.allAsList(futures).get();
        for (Message.ServerResponse<?> response: responses) {
            assertFalse(response.getRecord() instanceof Operation.Error);
        }
        
        client.stop().get();
    }
}
