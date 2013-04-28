package edu.uw.zookeeper.util;

import com.google.common.base.Throwables;
import com.typesafe.config.Config;

import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.Configuration;

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
        
        private final Factory<Config> configFactory = Configuration.DefaultConfigFactory.Singleton.getInstance();
        
        @Override
        public Configuration get(Arguments arguments) {
            Config defaultConfig = configFactory.get();
            return Configuration.create(arguments, defaultConfig);
        }
    }
    
    public static class DefaultApplicationFactory implements ParameterizedFactory<Configuration, Runnable> {

        public static DefaultApplicationFactory newInstance(Class<? extends Runnable> applicationType) {
            return new DefaultApplicationFactory(applicationType);
        }
        
        public static <T extends Runnable> T newApplication(Class<T> cls, Configuration configuration) {
            try {
                return cls.getConstructor(Configuration.class).newInstance(configuration);
            } catch(Exception e) {
                throw Throwables.propagate(e);
            }
        }

        private final Class<? extends Runnable> applicationType;
        
        private DefaultApplicationFactory(Class<? extends Runnable> applicationType) {
            this.applicationType = applicationType;
        }
        
        @Override
        public Runnable get(Configuration value) {
            value.asArguments().setProgramName(applicationType.getName());
            return newApplication(applicationType, value);
        }
    }
    
    protected ConfigurableMain() {
    }
}
