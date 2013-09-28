package edu.uw.zookeeper.client;

import com.google.common.base.Function;

import edu.uw.zookeeper.EnsembleView;
import edu.uw.zookeeper.ServerInetAddressView;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;

@Configurable(arg="servers", key="servers", value="127.0.0.1:2181", help="address:port,...")
public class ConfigurableEnsembleView implements Function<Configuration, EnsembleView<ServerInetAddressView>> {

    public static EnsembleView<ServerInetAddressView> get(Configuration configuration) {
        return new ConfigurableEnsembleView().apply(configuration);
    }
    
    @Override
    public EnsembleView<ServerInetAddressView> apply(Configuration configuration) {
        Configurable configurable = getClass().getAnnotation(Configurable.class);
        return EnsembleView.fromString(
                configuration.withConfigurable(configurable)
                    .getConfigOrEmpty(configurable.path())
                        .getString(configurable.key()));
    }
}
