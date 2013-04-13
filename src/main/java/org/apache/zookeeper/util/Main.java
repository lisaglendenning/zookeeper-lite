package org.apache.zookeeper.util;

import java.util.List;

import org.apache.zookeeper.util.Arguments;
import org.apache.zookeeper.util.Configuration;

import com.google.common.collect.Lists;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;

public class Main extends AbstractModule {

    public static void main(String[] args) throws Exception {
        Main main = get();
        main.apply(args);
    }

    public static Main get() {
        return new Main();
    }

    protected static Arguments theArguments;

    protected static Configuration theConfiguration;

    protected static Injector theInjector;

    protected Main() {
    }

    @Override
    protected void configure() {
    }

    @Provides
    @Singleton
    protected Main getMain() {
        return this;
    }

    @Provides
    @Singleton
    protected Arguments getArguments(Main main) {
        if (theArguments == null) {
            SimpleArguments arguments = new SimpleArguments();
            arguments.setMainClass(main.getClass());
            theArguments = arguments;
        }
        return theArguments;
    }

    @Provides
    @Singleton
    protected Configuration getConfiguration() {
        if (theConfiguration == null) {
            theConfiguration = Configuration.create();
        }
        return theConfiguration;
    }

    protected Injector getInjector(List<Module> modules) {
        modules.add(this);
        if (theInjector == null) {
            theInjector = Guice.createInjector(modules);
        }
        return theInjector;
    }

    protected Arguments parse(String[] args) {
        Arguments arguments = getArguments(this);
        arguments.setArgs(args);
        arguments.parse();
        if (arguments.helpOptionSet()) {
            System.out.println(arguments.getUsage());
            System.exit(0);
        }
        return arguments;
    }

    protected void apply(String[] args) throws Exception {
        String ARG_MODULE = "module";
        Arguments arguments = getArguments(this);
        arguments.add(arguments.newOption(ARG_MODULE, "ClassName"));
        arguments = parse(args);
        Injector injector = getInjector(modules());
        Application app = injector.getInstance(Application.class);
        app.call();
    }

    protected List<Module> modules() throws Exception {
        String ARG_MODULE = "module";
        Arguments arguments = getArguments(this);
        if (!arguments.hasValue(ARG_MODULE)) {
            System.err.println(arguments.getUsage());
            throw new IllegalStateException(String.format(
                    "Missing required argument: %s", ARG_MODULE));
        }
        String moduleName = arguments.getValue(ARG_MODULE);
        Module mainModule = (Module) Class.forName(moduleName).newInstance();
        return Lists.newArrayList(mainModule);
    }
}
