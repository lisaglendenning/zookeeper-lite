package edu.uw.zookeeper.server;

import java.net.UnknownHostException;

import com.google.common.base.Function;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

@Configurable(arg="clientAddress", key="clientAddress", value=":2181", help="address:port")
public class ConfigurableServerAddressView implements Function<Configuration, ServerInetAddressView> {

    public static ServerInetAddressView get(Configuration configuration) {
        return new ConfigurableServerAddressView().apply(configuration);
    }
    
    @Override
    public ServerInetAddressView apply(Configuration configuration) {
        Configurable configurable = getClass().getAnnotation(Configurable.class);
        String value = 
                configuration.withConfigurable(configurable)
                .getConfigOrEmpty(configurable.path())
                    .getString(configurable.key());
        try {
            return ServerInetAddressView.fromString(value);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException(value, e);
        }
    }
}