package edu.uw.zookeeper.client;


import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Application;
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
    
    protected static class MainBuilder extends ClientApplicationBuilder<Main> {

        @Override
        protected Main getApplication() {
            ServiceMonitor monitor = runtime.serviceMonitor();
            monitor.add(clientConnectionFactory);
            monitor.add(getDefaultClientConnectionExecutorService());
            return new Main(ServiceApplication.newInstance(monitor));
        }
    }
}
