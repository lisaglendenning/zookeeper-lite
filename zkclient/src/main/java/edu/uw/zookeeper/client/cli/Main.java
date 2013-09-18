package edu.uw.zookeeper.client.cli;


import static com.google.common.base.Preconditions.checkState;

import java.io.IOException;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.ServiceApplication;
import edu.uw.zookeeper.common.ServiceMonitor;

public class Main extends ZooKeeperApplication.ForwardingApplication {

    public static void main(String[] args) {
        ZooKeeperApplication.main(args, new MainBuilder());
    }

    protected Main(Application delegate) {
        super(delegate);
    }
    
    protected static class MainBuilder implements ZooKeeperApplication.RuntimeBuilder<Main, MainBuilder> {

    	protected static final String DESCRIPTION = "ZooKeeper CLI Client\nType '?' at the prompt to get started.";
    	
    	protected final RuntimeModule runtime;

        public MainBuilder() {
            this(null);
        }
        
        protected MainBuilder(RuntimeModule runtime) {
            this.runtime = runtime;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return runtime;
        }

        @Override
        public MainBuilder setRuntimeModule(RuntimeModule runtime) {
            return newInstance(runtime);
        }

        @Override
        public MainBuilder setDefaults() {
            checkState(runtime != null);
            return this;
        }

        @Override
        public Main build() {
            return setDefaults().doBuild();
        }

        protected Main doBuild() {
            getRuntimeModule().getConfiguration().getArguments().setDescription(DESCRIPTION);
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            Shell shell;
            try {
                shell = Shell.create(runtime);
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            monitor.add(shell);
            monitor.add(DispatchingInvoker.defaults(shell));
            return new Main(ServiceApplication.newInstance(monitor));
        }

        protected MainBuilder newInstance(RuntimeModule runtime) {
            return new MainBuilder(runtime);
        }
    }
}
