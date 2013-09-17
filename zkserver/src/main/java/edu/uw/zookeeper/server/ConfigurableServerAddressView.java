package edu.uw.zookeeper.server;

import com.google.common.base.Function;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

@Configurable(arg="clientAddress", key="ClientAddress", value=":2181", help="Address:Port")
public class ConfigurableServerAddressView implements Function<Configuration, ServerInetAddressView> {

    public static ServerInetAddressView get(Configuration configuration) {
        return new ConfigurableServerAddressView().apply(configuration);
    }
    
    @Override
    public ServerInetAddressView apply(Configuration configuration) {
        Configurable configurable = getClass().getAnnotation(Configurable.class);
        return ServerInetAddressView.fromString(
                configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                        .getString(configurable.key()));
    }
}