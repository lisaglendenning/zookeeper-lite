package edu.uw.zookeeper.client.console;


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
    
    protected static class MainBuilder extends ZooKeeperApplication.ForwardingBuilder<Main, ConsoleClientBuilder, MainBuilder> {

    	protected static final String DESCRIPTION = "ZooKeeper Shell Client";
    	
        public MainBuilder() {
            this(ConsoleClientBuilder.defaults());
        }

        public MainBuilder(
                ConsoleClientBuilder delegate) {
            super(delegate);
        }

        @Override
        protected MainBuilder newInstance(ConsoleClientBuilder delegate) {
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
