package edu.uw.zookeeper.server;


import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.util.concurrent.Service;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.protocol.server.ServerConnectionsHandler;

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

    protected static class MainBuilder implements ZooKeeperApplication.RuntimeBuilder<Main, MainBuilder> {
        
    	protected static final String DESCRIPTION = "ZooKeeper Standalone In-Memory Server";
    	
    	protected final SimpleServerExecutor.Builder server;
        protected final ServerConnectionsHandler.Builder connections;
    	
        public MainBuilder() {
            this(SimpleServerExecutor.builder(), ServerConnectionsHandler.builder());
        }

        public MainBuilder(
                SimpleServerExecutor.Builder server,
                ServerConnectionsHandler.Builder connections) {
            this.server = checkNotNull(server);
            this.connections = checkNotNull(connections);
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return server.getRuntimeModule();
        }

        @Override
        public MainBuilder setRuntimeModule(RuntimeModule runtime) {
            return newInstance(server.setRuntimeModule(runtime), connections.setRuntimeModule(runtime));
        }

        @Override
        public MainBuilder setDefaults() {
            SimpleServerExecutor.Builder server = this.server.setDefaults();
            if (server != this.server) {
                return newInstance(server, connections).setDefaults();
            }
            if (connections.getServerExecutor() == null) {
                return newInstance(server, connections.setServerExecutor(server.build())).setDefaults();
            }
            ServerConnectionsHandler.Builder connections = this.connections.setDefaults();
            if (connections != this.connections) {
                return newInstance(server, connections).setDefaults();
            }
            return this;
        }

        @Override
        public Main build() {
            return setDefaults().doBuild();
        }

        protected MainBuilder newInstance(
                SimpleServerExecutor.Builder server,
                ServerConnectionsHandler.Builder connections) {
            return new MainBuilder(server, connections);
        }

        protected Main doBuild() {
            getRuntimeModule().getConfiguration().getArguments().setDescription(DESCRIPTION);
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            for (Service service: connections.build()) {
                monitor.add(service);
            }
            return new Main(ServiceApplication.newInstance(monitor));
        }
    }
}
