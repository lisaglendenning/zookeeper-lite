package edu.uw.zookeeper.common;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;

import edu.uw.zookeeper.common.Arguments;
import edu.uw.zookeeper.common.Configuration;

public abstract class ConfigurableMain {

    public static void exitIfHelpSet(Arguments arguments) {
        arguments.parse();
        if (arguments.helpOptionSet()) {
            System.out.println(arguments.getUsage());
            System.exit(0);
        }        
    }

    public static <T extends Runnable> void main(String[] args, ParameterizedFactory<Configuration, T> applicationFactory) {
        Arguments arguments = DefaultArgumentsFactory.getInstance().get(args);
        Configuration configuration = DefaultConfigurationFactory.getInstance().get(arguments);
        T application = applicationFactory.get(configuration);
        application.run();
    }
    
    public static enum DefaultArgumentsFactory implements DefaultsFactory<String[], Arguments> {
        INSTANCE;
        
        public static DefaultArgumentsFactory getInstance() {
            return INSTANCE;
        }
        
        @Override
        public SimpleArguments get() {
            return get(new String[0]);
        }

        @Override
        public SimpleArguments get(String[] value) {
            // TODO Auto-generated method stub
            SimpleArguments arguments = SimpleArguments.create();
            arguments.setArgs(value);
            return arguments;
        }
    }
    
    public static enum DefaultConfigurationFactory implements ParameterizedFactory<Arguments, Configuration> {
        INSTANCE;
        
        public static DefaultConfigurationFactory getInstance() {
            return INSTANCE;
        }
        
        private final Factory<Config> configFactory = Configuration.DefaultConfigFactory.Holder.getInstance();
        
        @Override
        public Configuration get(Arguments arguments) {
            Config defaultConfig = configFactory.get();
            return Configuration.create(arguments, defaultConfig);
        }
    }
    
    public static class ConfigurableApplicationFactory<T extends Runnable> implements ParameterizedFactory<Configuration, T> {

        public static <T extends Runnable> ConfigurableApplicationFactory<T> newInstance(Class<? extends T> applicationType) {
            return new ConfigurableApplicationFactory<T>(applicationType);
        }
        
        public static <T extends Runnable> T newApplication(Class<T> cls, Configuration configuration) {
            try {
                return cls.getConstructor(Configuration.class).newInstance(configuration);
            } catch(Exception e) {
                throw Throwables.propagate(e);
            }
        }

        private final Class<? extends T> applicationType;
        
        private ConfigurableApplicationFactory(Class<? extends T> applicationType) {
            this.applicationType = applicationType;
        }
        
        @Override
        public T get(Configuration value) {
            value.asArguments().setProgramName(applicationType.getName());
            return newApplication(applicationType, value);
        }
    }
    
    private ConfigurableMain() {}
}