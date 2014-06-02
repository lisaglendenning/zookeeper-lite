package edu.uw.zookeeper.server;

import java.net.UnknownHostException;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

@Configurable(path="server", arg="clientAddress", key="clientAddress", value=":2181", help="address:port")
public abstract class ClientAddressConfiguration {

    public static Configurable getConfigurable() {
        return ClientAddressConfiguration.class.getAnnotation(Configurable.class);
    }
    
    public static ServerInetAddressView get(Configuration configuration) {
        Configurable configurable = getConfigurable();
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
    
    public static Configuration set(Configuration configuration, ServerInetAddressView value) {
        Configurable configurable = getConfigurable();
        return configuration.withConfig(ConfigFactory.parseMap(ImmutableMap.<String,Object>builder().put(ConfigUtil.joinPath(configurable.path(), configurable.key()), value.toString()).build()));
    }
    
    protected ClientAddressConfiguration() {}
}