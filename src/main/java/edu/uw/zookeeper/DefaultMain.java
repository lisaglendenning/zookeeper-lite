package edu.uw.zookeeper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.UncaughtExceptionHandlers;

import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Arguments;
import edu.uw.zookeeper.common.Factories;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;


public class DefaultMain implements Application {
    
    public static void main(String[] args, ParameterizedFactory<RuntimeModule, ? extends Application> factory) {
        create(DefaultRuntimeModule.fromArgs(args), factory).run();
    }

    public static DefaultMain create(
            RuntimeModule runtime,
            ParameterizedFactory<? super RuntimeModule, ? extends Application> factory) {
        return new DefaultMain(runtime, factory);
    }

    public static void exitIfHelpSet(Arguments arguments) {
        arguments.parse();
        if (arguments.helpOptionSet()) {
            System.out.println(arguments.getUsage());
            System.exit(0);
        }        
    }

    protected final Logger logger;
    protected final RuntimeModule runtime;
    protected final Supplier<? extends Application> application;

    public DefaultMain(
            RuntimeModule runtime,
            ParameterizedFactory<? super RuntimeModule, ? extends Application> factory) {
        this(runtime, Factories.synchronizedLazyFrom(Factories.applied(factory, runtime)));
    }

    public DefaultMain(
            RuntimeModule runtime,
            Supplier<? extends Application> application) {
        super();
        this.logger = LogManager.getLogger(getClass());
        this.runtime = runtime;
        this.application = application;
    }

    public Application getApplication() {
        return application.get();
    }

    public RuntimeModule getRuntimeModule() {
        return runtime;
    }

    @Override
    public void run() {
        Thread.currentThread().setUncaughtExceptionHandler(UncaughtExceptionHandlers.systemExit());
        Application application = this.application.get();
        if (runtime.configuration().getArguments().getProgramName().isEmpty()) {
            runtime.configuration().getArguments().setProgramName(application.getClass().getName());
        }
        exitIfHelpSet(runtime.configuration().getArguments());
        logger.info("{}", runtime.configuration().getConfig());
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                runtime.shutdown();
            }
        });
        application.run();
        logger.info("Exiting");
    }
}
