package edu.uw.zookeeper;

import static org.junit.Assert.assertFalse;

import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.client.BasicOperationGenerator;
import edu.uw.zookeeper.client.PipeliningClient;
import edu.uw.zookeeper.client.SimpleClient;
import edu.uw.zookeeper.client.ZNodeViewCache;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;

@RunWith(JUnit4.class)
public class RandomSingleClientTest {

    @Test(timeout=10000)
    public void testRandom() throws InterruptedException, ExecutionException, KeeperException {
        SimpleClient client = SimpleClient.newInstance();

        client.start().get();
        
        ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache = 
                ZNodeViewCache.newInstance(client.getClient(), client.getClient());
        PipeliningClient<Records.Request, Message.ServerResponse<?>> operations = PipeliningClient.create(100, cache, BasicOperationGenerator.create(cache));
        List<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> futures = operations.next();
        for (Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>> future: futures) {
            assertFalse(future.second().get().getRecord() instanceof Operation.Error);
        }
        
        client.stop().get();
    }
}
