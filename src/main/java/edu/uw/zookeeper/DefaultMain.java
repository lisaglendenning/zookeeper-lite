package edu.uw.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;

import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Arguments;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.Publisher;
import edu.uw.zookeeper.common.ServiceMonitor;
import edu.uw.zookeeper.common.Singleton;
import edu.uw.zookeeper.common.TimeValue;


public class DefaultMain extends DefaultRuntimeModule implements Application {
    
    public static Singleton<Application> application(
            final ParameterizedFactory<RuntimeModule, Application> applicationFactory,
            final RuntimeModule runtime) {
        return Factories.synchronizedLazyFrom(new Factory<Application>() {
            @Override
            public Application get() {
                return applicationFactory.get(runtime);
            }
        });
    }
    
    @Configurable(arg="timeout", key="Timeout", value="30 seconds", help="Time")
    public static class ConfigurableTimeout implements Function<Configuration, TimeValue> {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableTimeout().apply(configuration);
        }

        @Override
        public TimeValue apply(Configuration configuration) {
            Configurable configurable = getClass().getAnnotation(Configurable.class);
            return TimeValue.fromString(
                    configuration.withConfigurable(configurable)
                        .getConfigOrEmpty(configurable.path())
                            .getString(configurable.key()));
        }
    }

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
    
    protected final Logger logger = LogManager.getLogger(getClass());
    protected final Singleton<Application> application;

    public DefaultMain(
            final ParameterizedFactory<RuntimeModule, Application> applicationFactory) {
        super();
        this.application = application(applicationFactory, this);
    }
    
    public DefaultMain(
            ParameterizedFactory<RuntimeModule, Application> applicationFactory,
            Configuration configuration) {
        super(configuration);
        this.application = application(applicationFactory, this);
    }
    
    public DefaultMain(
            ParameterizedFactory<RuntimeModule, Application> applicationFactory,
            Configuration configuration,
            Factory<? extends Publisher> publishers,
            ServiceMonitor serviceMonitor, 
            TimeValue shutdownTimeout) {
        super(configuration, publishers, serviceMonitor, shutdownTimeout);
        this.application = application(applicationFactory, this);
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit());
        Application application = application();
        exitIfHelpSet(configuration().getArguments());
        logger.info("{}", configuration().getConfig());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdown();
            }
        });
        application.run();
        logger.info("Exiting");
    }

    public Application application() {
        return application.get();
    }
}
