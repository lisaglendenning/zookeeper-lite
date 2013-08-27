package edu.uw.zookeeper.common;

import com.google.common.base.Throwables;

import edu.uw.zookeeper.DefaultRuntimeModule;
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
        T application = applicationFactory.get(DefaultRuntimeModule.configuration(args));
        application.run();
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
            value.getArguments().setProgramName(applicationType.getName());
            return newApplication(applicationType, value);
        }
    }
    
    private ConfigurableMain() {}
}
