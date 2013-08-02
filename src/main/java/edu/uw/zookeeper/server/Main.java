package edu.uw.zookeeper.server;


import edu.uw.zookeeper.AbstractMain;
import edu.uw.zookeeper.common.ConfigurableMain;
import edu.uw.zookeeper.common.Configuration;

public class Main extends AbstractMain {

    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.ConfigurableApplicationFactory.newInstance(Main.class));
    }

    public Main(Configuration configuration) {
        super(configuration, ServerApplicationModule.getInstance());
    }
}
