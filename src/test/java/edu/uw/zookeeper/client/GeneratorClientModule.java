package edu.uw.zookeeper.client;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ListenableFuture;

import edu.uw.zookeeper.DefaultMain;
import edu.uw.zookeeper.RuntimeModule;
import edu.uw.zookeeper.common.ConfigurableMain;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Pair;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.data.ZNodeLabel;
import edu.uw.zookeeper.net.Connection;
import edu.uw.zookeeper.protocol.Message;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.client.AssignXidCodec;
import edu.uw.zookeeper.protocol.client.ClientConnectionExecutorService;
import edu.uw.zookeeper.protocol.client.PingingClient;
import edu.uw.zookeeper.protocol.proto.Records;

public class GeneratorClientModule extends ClientApplicationModule {

    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.ConfigurableApplicationFactory.newInstance(Main.class));
    }
    
    public static class Main extends DefaultMain {
        public Main(Configuration configuration) {
            super(GeneratorClientModule.getInstance(), configuration);
        }
    }
 
    public static GeneratorClientModule getInstance() {
        return new GeneratorClientModule();
    }

    @Override
    protected ServiceMonitor createServices(RuntimeModule runtime) {
        runtime.serviceMonitor().add(
                RunnableService.create(getGenerator(runtime)));
        return runtime.serviceMonitor();
    }
    
    protected ZNodeViewCache <?, Operation.Request, Message.ServerResponse<?>> getCache(RuntimeModule runtime) {
        ClientConnectionExecutorService<PingingClient<Operation.Request, AssignXidCodec, Connection<Operation.Request>>> client = getClientConnectionExecutorService(runtime);
        ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache = ZNodeViewCache.newInstance(client, client);
        return cache;
    }
    
    protected Generator<Records.Request> getRequests(RuntimeModule runtime, ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache) {
        return PathedOperationGenerator.create(cache);
    }

    protected Runnable getGenerator(final RuntimeModule runtime) {
        final ZNodeViewCache<?, Operation.Request, Message.ServerResponse<?>> cache = getCache(runtime);
        final Generator<Records.Request> requests = getRequests(runtime, cache);
        final LimitOutstandingClient<Operation.Request, Message.ServerResponse<?>> limiting = LimitOutstandingClient.create(runtime.configuration(), cache);
        final CallUntilPresent<Pair<Records.Request, ListenableFuture<Message.ServerResponse<?>>>> callable = 
                    CallUntilPresent.create(
                        IteratingCallable.create(runtime.configuration(), 
                                SubmitCallable.create(requests, limiting)));
        return new Runnable() {
            @Override
            public void run() {
                try {
                    TreeFetcher.builder().setClient(cache).build().apply(ZNodeLabel.Path.root()).get();
                    callable.call().second().get();
                } catch (Exception e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
