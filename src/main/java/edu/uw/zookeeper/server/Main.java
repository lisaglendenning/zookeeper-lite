package edu.uw.zookeeper.server;


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
    
    protected static class MainBuilder extends ServerApplicationBuilder<Main> {

        @Override
        protected Main getApplication() {
            ServiceMonitor monitor = runtime.serviceMonitor();
            monitor.add(serverConnectionFactory);
            monitor.add(getDefaultServerConnectionExecutorsService());
            return new Main(ServiceApplication.newInstance(monitor));
        }
    }
}
