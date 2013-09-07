package edu.uw.zookeeper.client;


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
    
    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<Main, ClientBuilder, MainBuilder> {

    	protected static final String DESCRIPTION = "ZooKeeper Client";
    	
        public MainBuilder() {
            this(ClientBuilder.defaults());
        }

        public MainBuilder(
                ClientBuilder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(ClientBuilder delegate) {
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
