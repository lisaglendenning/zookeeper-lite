package edu.uw.zookeeper.server;


import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.protocol.server.ServerConnectionExecutorsService;

public class Main extends ZooKeeperApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected final Application application;
    
    protected Main(Application application) {
        super();
        this.application = application;
    }

    @Override
    public void run() {
        application.run();
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<Main, ServerConnectionExecutorsService.Builder, MainBuilder> {
        
    	protected static final String DESCRIPTION = "ZooKeeper Standalone In-Memory Server";
    	
        public MainBuilder() {
            this(ServerConnectionExecutorsService.builder());
        }

        public MainBuilder(
                ServerConnectionExecutorsService.Builder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(ServerConnectionExecutorsService.Builder delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected Main doBuild() {
        	getRuntimeModule().getConfiguration().getArguments().setDescription(DESCRIPTION);
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new Main(ServiceApplication.newInstance(monitor));
        }
    }
}
