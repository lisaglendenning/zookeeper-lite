package edu.uw.zookeeper.client.cli;


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
    
    protected static class MainBuilder extends ZooKeeperApplication.AbstractRuntimeBuilder<Main, MainBuilder> {

    	protected static final String DESCRIPTION = "ZooKeeper CLI Client\nType '?' at the prompt to get started.";
    	
        public MainBuilder() {
            this(null);
        }
        
        protected MainBuilder(RuntimeModule runtime) {
            super(runtime);
        }

        @Override
        protected Main doBuild() {
            getRuntimeModule().getConfiguration().getArguments().setDescription(getDescription());
            Shell shell;
            try {
                shell = newShell();
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
            return newMain(shell);
        }

        @Override
        protected MainBuilder newInstance(RuntimeModule runtime) {
            return new MainBuilder(runtime);
        }

        protected String getDescription() {
            return DESCRIPTION;
        }
        
        protected Shell newShell() throws IOException {
            ServiceMonitor monitor = getRuntimeModule().getServiceMonitor();
            Shell shell = Shell.create(getRuntimeModule());
            monitor.add(shell);
            monitor.add(newDispatchingInvoker(shell));
            return shell;
        }
        
        protected DispatchingInvoker newDispatchingInvoker(Shell shell) {
            return DispatchingInvoker.defaults(shell);
        }
        
        protected Main newMain(Shell shell) {
            return new Main(ServiceApplication.forService(getRuntimeModule().getServiceMonitor()));
        }
    }
}
