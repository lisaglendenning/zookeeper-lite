package edu.uw.zookeeper.server;


import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class Main extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected Main(Application delegate) {
        super(delegate);
    }

    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<Main, SimpleServerBuilder<?>, MainBuilder> {
        
    	protected static final String DESCRIPTION = "ZooKeeper Standalone In-Memory Server";
    	
        public MainBuilder() {
            this(SimpleServerBuilder.defaults());
        }

        public MainBuilder(SimpleServerBuilder<?> delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(SimpleServerBuilder<?> delegate) {
            return new MainBuilder(delegate);
        }

        @Override
        protected Main doBuild() {
            getRuntimeModule().getConfiguration().getArguments().setDescription(getDescription());
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new Main(ServiceApplication.forService(monitor));
        }

        protected String getDescription() {
            return DESCRIPTION;
        }
    }
}
