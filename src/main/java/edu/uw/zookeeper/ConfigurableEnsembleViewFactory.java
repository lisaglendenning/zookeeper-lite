package edu.uw.zookeeper;

import java.util.Map;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;

public class ConfigurableEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleView> {

    public static ConfigurableEnsembleViewFactory newInstance() {
        return new ConfigurableEnsembleViewFactory("");
    }
    
    public static final String ARG = "ensemble";
    public static final String CONFIG_KEY = "Ensemble";
    public static final String DEFAULT_ADDRESS = "localhost";
    public static final int DEFAULT_PORT = 2181;

    private final String configPath;
    
    protected ConfigurableEnsembleViewFactory(String configPath) {
        this.configPath = configPath;
    }
    
    @Override
    public EnsembleView get() {
        ServerInetView localhost = ServerInetView.newInstance(DEFAULT_ADDRESS, DEFAULT_PORT);
        return EnsembleView.of(ServerQuorumView.newInstance(localhost));
    }

    @Override
    public EnsembleView get(Configuration value) {
        Arguments arguments = value.asArguments();
        if (! arguments.has(ARG)) {
            arguments.add(arguments.newOption(ARG, "Ensemble"));
        }
        arguments.parse();
        Map<String, Object> args = Maps.newHashMap();
        if (arguments.hasValue(ARG)) {
            args.put(CONFIG_KEY, arguments.getValue(ARG));
        }
        
        Config config = value.asConfig();
        if (configPath.length() > 0 && config.hasPath(configPath)) {
            config = config.getConfig(configPath);
        } else {
            config = ConfigFactory.empty();
        }
        if (! args.isEmpty()) {
            config = ConfigValueFactory.fromMap(args).toConfig().withFallback(config);
        }
       
        if (config.hasPath(CONFIG_KEY)) {
        String input = config.getString(CONFIG_KEY);
            try {
                return EnsembleView.fromString(input);
            } catch (ClassNotFoundException e) {
                throw Throwables.propagate(e);
            }
        } else {
            return get();
        }
    }
}