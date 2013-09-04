package edu.uw.zookeeper.client;


import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

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
    
    protected static class MainBuilder implements ZooKeeperApplication.RuntimeBuilder<Main> {
        
        protected final ClientBuilder delegate;
        
        public MainBuilder() {
            this(ClientBuilder.defaults());
        }

        public MainBuilder(
                ClientBuilder delegate) {
            this.delegate = delegate;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return delegate.getRuntimeModule();
        }

        @Override
        public MainBuilder setRuntimeModule(
                RuntimeModule runtime) {
            return new MainBuilder(delegate.setRuntimeModule(runtime));
        }

        @Override
        public Main build() {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: delegate.build()) {
                monitor.add(service);
            }
            return new Main(ServiceApplication.newInstance(monitor));
        }
    }
}
