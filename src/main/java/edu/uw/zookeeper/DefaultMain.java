package edu.uw.zookeeper;

import com.google.common.util.concurrent.UncaughtExceptionHandlers;

import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.ConfigurableMain;
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
        return Factories.lazyFrom(new Factory<Application>() {
            @Override
            public Application get() {
                return applicationFactory.get(runtime);
            }
        });
    }
    
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
        ConfigurableMain.exitIfHelpSet(configuration().asArguments());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                shutdown();
            }
        });
        application.run();
    }

    public Application application() {
        return application.get();
    }
}
