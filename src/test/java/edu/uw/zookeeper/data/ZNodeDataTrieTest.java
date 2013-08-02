package edu.uw.zookeeper.data;

import java.util.concurrent.ExecutionException;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import edu.uw.zookeeper.client.SessionClientProcessor;

@RunWith(JUnit4.class)
public class ZNodeDataTrieTest {
    
    @Test
    public void test() throws InterruptedException, ExecutionException {
        ZNodeDataTrieExecutor executor = ZNodeDataTrieExecutor.create();
        SessionClientProcessor sessionProcessor = SessionClientProcessor.create(1);
        
        
        ZNodeLabel.Path path = ZNodeLabel.Path.of("/foo");
        executor.submit(sessionProcessor.apply(Operations.Requests.create().setPath(path).build())).get();
        executor.submit(sessionProcessor.apply(Operations.Requests.exists().setPath(path).build())).get();
        executor.submit(sessionProcessor.apply(Operations.Requests.delete().setPath(path).build())).get();
    }
}
