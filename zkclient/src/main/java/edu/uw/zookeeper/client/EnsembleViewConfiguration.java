package edu.uw.zookeeper.client;

import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigUtil;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

@Configurable(path="client", arg="servers", value="127.0.0.1:2181", help="address:port,...")
public abstract class EnsembleViewConfiguration {

    public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
        Configurable configurable = getConfigurable();
        String value = 
                configuration.withConfigurable(configurable)
                .getConfigOrEmpty(configurable.path())
                    .getString(configurable.arg());
        return ServerInetAddressView.ensembleFromString(value);
    }

    public static Configurable getConfigurable() {
        return EnsembleViewConfiguration.class.getAnnotation(Configurable.class);
    }
    
    public static Configuration set(Configuration configuration, EnsembleView<ServerInetAddressView> value) {
        Configurable configurable = getConfigurable();
        return configuration.withConfig(ConfigFactory.parseMap(ImmutableMap.<String,Object>builder().put(ConfigUtil.joinPath(configurable.path(), configurable.arg()), EnsembleView.toString(value)).build()));
    }
    
    protected EnsembleViewConfiguration() {}
}
