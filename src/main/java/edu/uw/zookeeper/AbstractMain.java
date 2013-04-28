package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.Service;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

import edu.uw.zookeeper.util.Application;
import edu.uw.zookeeper.util.Arguments;
import edu.uw.zookeeper.util.ConfigurableMain;
import edu.uw.zookeeper.util.Configuration;
import edu.uw.zookeeper.util.DefaultsFactory;
import edu.uw.zookeeper.util.EventBusPublisher;
import edu.uw.zookeeper.util.ExecutorServiceMonitor;
import edu.uw.zookeeper.util.Factories;
import edu.uw.zookeeper.util.Factory;
import edu.uw.zookeeper.util.ParameterizedFactory;
import edu.uw.zookeeper.util.Publisher;
import edu.uw.zookeeper.util.ServiceMonitor;
import edu.uw.zookeeper.util.Singleton;


public abstract class AbstractMain implements Runnable {

    public static enum PublisherFactory implements Factory<Publisher> {
        INSTANCE;

        public static PublisherFactory getInstance() {
            return INSTANCE;
        }

        @Override
        public Publisher get() {
            return new EventBusPublisher();
        }        
    }
    
    public static enum PlatformThreadFactory implements Factory<ThreadFactory> {
        INSTANCE;

        public static PlatformThreadFactory getInstance() {
            return INSTANCE;
        }
        
        @Override
        public ThreadFactory get() {
            return MoreExecutors.platformThreadFactory();
        }
    }
    
    public static enum SingleDaemonThreadScheduledExectorFactory implements DefaultsFactory<ThreadFactory, ScheduledExecutorService> {
        INSTANCE;

        public static SingleDaemonThreadScheduledExectorFactory getInstance() {
            return INSTANCE;
        }
        
        private final String nameFormat = "scheduled-%d";

        @Override
        public ScheduledExecutorService get() {
            return get(PlatformThreadFactory.getInstance().get());
        }
        
        @Override
        public ScheduledExecutorService get(ThreadFactory threadFactory) {
            return Executors.newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder()
                    .setThreadFactory(threadFactory)
                    .setDaemon(true)
                    .setNameFormat(nameFormat)
                    .build());
        }
    }
    
    public static enum ApplicationExecutorFactory implements DefaultsFactory<ThreadFactory, ExecutorService> {
        INSTANCE;

        public static ApplicationExecutorFactory getInstance() {
            return INSTANCE;
        }
        
        // TODO: configurable
        private final int CORE_SIZE = Math.max(1,
                Runtime.getRuntime().availableProcessors() * 2);
        private final String nameFormat = "application-%d";

        @Override
        public ExecutorService get() {
            return get(PlatformThreadFactory.getInstance().get());
        }
        
        public ExecutorService get(ThreadFactory threadFactory) {
            return Executors.newFixedThreadPool(CORE_SIZE,
                    new ThreadFactoryBuilder()
                        .setThreadFactory(threadFactory)
                        .setNameFormat(nameFormat)
                        .build());
        }
    }
    
    public static ListeningExecutorServiceFactory listeningExecutors(ServiceMonitor serviceMonitor) {
        return ListeningExecutorServiceFactory.newInstance(
                serviceMonitor,
                ImmutableMap.<Class<? extends ExecutorService>, Factory<? extends ExecutorService>>of(
                        ScheduledExecutorService.class, SingleDaemonThreadScheduledExectorFactory.getInstance(),
                        ExecutorService.class, ApplicationExecutorFactory.getInstance()));
    }
    
    public static class ExecutorServiceFactory implements ParameterizedFactory<Class<? extends ExecutorService>, ExecutorService> {

        protected class TypeView<T extends ExecutorService> implements Singleton<T> {
            protected final Class<T> type;
            
            public TypeView(Class<T> type) {
                this.type = type;
            }
            
            @SuppressWarnings("unchecked")
            @Override
            public T get() {
                return (T) ExecutorServiceFactory.this.get(type);
            }
        }
        
        public static ExecutorServiceFactory newInstance(
                ServiceMonitor serviceMonitor,
                Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
            return new ExecutorServiceFactory(serviceMonitor, factories);
        }
        
        protected final ServiceMonitor serviceMonitor;
        protected final Factories.ByTypeFactory<ExecutorService> factory;
        protected final Map<Class<? extends ExecutorService>, ExecutorServiceMonitor<?>> instances;
        
        protected ExecutorServiceFactory(
                ServiceMonitor serviceMonitor,
                Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
            checkArgument(! factories.isEmpty());
            this.serviceMonitor = serviceMonitor;
            this.factory = Factories.ByTypeFactory.newInstance(factories);
            this.instances = Maps.newHashMap();
        }
        
        public Singleton<ExecutorService> asExecutorServiceFactory() {
            return newView(ExecutorService.class);
        }

        public Singleton<ScheduledExecutorService> asScheduledExecutorServiceFactory() {
            return newView(ScheduledExecutorService.class);
        }
        
        protected <T extends ExecutorService> TypeView<T> newView(Class<T> type) {
            return new TypeView<T>(type);
        }
        
        @Override
        public synchronized ExecutorService get(Class<? extends ExecutorService> type) {
            ExecutorServiceMonitor<?> instance = instances.get(type);
            if (instance == null) {
                instance = newInstance(type);
                instances.put(type, instance);
                serviceMonitor.add(instance);
            }
            return instance.get();
        }
        
        protected ExecutorServiceMonitor<?> newInstance(Class<? extends ExecutorService> type) {
            ExecutorService executor = factory.get(type);
            checkArgument(executor != null);
            ExecutorServiceMonitor<?> instance = ExecutorServiceMonitor.newInstance(executor);
            return instance;
        }
    }
    
    /**
     * Adds Listening wrapper around executor factory, and maps both a listening
     * and non-listening interface to the same backing Executor.
     */
    public static class ListeningExecutorServiceFactory extends ExecutorServiceFactory {

        public static ListeningExecutorServiceFactory newInstance(
                ServiceMonitor serviceMonitor,
                Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
            return new ListeningExecutorServiceFactory(serviceMonitor, factories);
        }
        
        protected ListeningExecutorServiceFactory(
                ServiceMonitor serviceMonitor,
                Map<Class<? extends ExecutorService>, Factory<? extends ExecutorService>> factories) {
            super(serviceMonitor, factories);
        }
        
        public Singleton<ListeningExecutorService> asListeningExecutorServiceFactory() {
            return newView(ListeningExecutorService.class);
        }

        public Singleton<ListeningScheduledExecutorService> asListeningScheduledExecutorServiceFactory() {
            return newView(ListeningScheduledExecutorService.class);
        }
        
        @Override
        protected ExecutorServiceMonitor<?> newInstance(Class<? extends ExecutorService> type) {
            // create executor
            ExecutorService executor = factory.get(type);
            if (executor == null) {
                if (ScheduledExecutorService.class.isAssignableFrom(type)) {
                    executor = factory.get(ListeningScheduledExecutorService.class);
                } else {
                    executor = factory.get(ListeningExecutorService.class);
                }            
            }
            checkArgument(executor != null);
            
            // wrap with listening interface
            if (! (executor instanceof ListeningExecutorService)) {
                if (ScheduledExecutorService.class.isAssignableFrom(type)) {
                    executor = MoreExecutors.listeningDecorator((ScheduledExecutorService)executor);
                } else {
                    executor = MoreExecutors.listeningDecorator((ScheduledExecutorService)executor);
                }
            }
            
            // wrap with service
            ExecutorServiceMonitor<?> instance = ExecutorServiceMonitor.newInstance(executor);
            
            // add extra lookup
            Class<? extends ExecutorService> extraType;
            if (ScheduledExecutorService.class.isAssignableFrom(type)) {
                if (ListeningScheduledExecutorService.class.isAssignableFrom(type)) {
                    extraType = ScheduledExecutorService.class;
                } else {
                    extraType = ListeningScheduledExecutorService.class;
                }
            } else {
                if (ListeningExecutorService.class.isAssignableFrom(type)) {
                    extraType = ExecutorService.class;
                } else {
                    extraType = ListeningExecutorService.class;
                }
            }
            assert(! type.equals(extraType));
            checkArgument(! instances.containsKey(extraType));
            instances.put(extraType, instance);
            
            return instance;
        }
    }

    public static MonitorServiceFactory monitors(ServiceMonitor serviceMonitor) {
        return MonitorServiceFactory.newInstance(serviceMonitor);
    }
    
    public static class MonitorServiceFactory implements ParameterizedFactory<Service, Service> {

        public static <T extends Service> MonitorServiceFactory newInstance(
                ServiceMonitor serviceMonitor) {
            return new MonitorServiceFactory(serviceMonitor);
        }
        
        protected final ServiceMonitor serviceMonitor;
        
        protected MonitorServiceFactory(
                ServiceMonitor serviceMonitor) {
            this.serviceMonitor = serviceMonitor;
        }

        @Override
        public Service get(Service value) {
            serviceMonitor.add(value);
            return value;
        }
    }
    
    public static class ConfigurableEnsembleViewFactory implements DefaultsFactory<Configuration, EnsembleView> {

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
    
    public static void main(String[] args) {
        ConfigurableMain.main(args, ConfigurableMain.DefaultApplicationFactory.newInstance(AbstractMain.class));
    }
    
    protected final Factory<Publisher> publisherFactory;
    protected final Singleton<ServiceMonitor> serviceMonitor;
    protected final Singleton<Configuration> configuration;
    protected final ListeningExecutorServiceFactory executors;
    
    protected AbstractMain(Configuration configuration) {
        this.configuration = Factories.holderOf(configuration);
        this.publisherFactory = PublisherFactory.getInstance();
        this.serviceMonitor = Factories.holderOf(ServiceMonitor.newInstance());
        this.executors = listeningExecutors(serviceMonitor.get());
    }
    
    public Configuration configuration() {
        return configuration.get();
    }
    
    public Factory<ThreadFactory> threadFactory() {
        return PlatformThreadFactory.getInstance();
    }
    
    public ServiceMonitor serviceMonitor() {
        return serviceMonitor.get();
    }
    
    public Factory<Publisher> publisherFactory() {
        return publisherFactory;
    }
    
    public ListeningExecutorServiceFactory executors() {
        return executors;
    }
    
    @Override
    public void run() {
        Application application = application();
        ConfigurableMain.exitIfHelpSet(configuration().asArguments());
        application.run();
    }

    protected abstract Application application();
}
