package edu.uw.zookeeper;


import com.google.common.base.Function;

import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Builder;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;
import edu.uw.zookeeper.common.TimeValue;

public abstract class ZooKeeperApplication implements Application {

    public static <T extends RuntimeBuilder<? extends Application, ?>> void main(String[] args, T builder) {
        DefaultMain.main(args, factory(builder));
    }
 
    public static <T extends RuntimeBuilder<? extends Application, ?>> ApplicationFactory<T> factory(T builder) {
        return ApplicationFactory.create(builder);
    }
    
    public static interface RuntimeBuilder<T, C extends RuntimeBuilder<T,C>> extends Builder<T> {
        
        RuntimeModule getRuntimeModule();
        
        C setRuntimeModule(RuntimeModule runtime);
        
        C setDefaults();
    }

    public static class ApplicationFactory<T extends RuntimeBuilder<? extends Application, ?>> implements ParameterizedFactory<RuntimeModule, Application> {

        public static <T extends RuntimeBuilder<? extends Application, ?>> ApplicationFactory<T> create(T builder) {
            return new ApplicationFactory<T>(builder);
        }
        
        protected final T builder;
        
        public ApplicationFactory(T builder) {
            this.builder = builder;
        }
        
        @Override
        public Application get(RuntimeModule runtime) {
            return builder.setRuntimeModule(runtime).setDefaults().build();
        }
    }
    
    public static abstract class ForwardingBuilder<V, T extends RuntimeBuilder<?,?>, C extends ForwardingBuilder<V,T,C>> implements RuntimeBuilder<V,C> {

        protected final T delegate;
        
        public ForwardingBuilder(T delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public RuntimeModule getRuntimeModule() {
            return delegate.getRuntimeModule();
        }

        @SuppressWarnings("unchecked")
        @Override
        public C setRuntimeModule(RuntimeModule runtime) {
            return newInstance((T) delegate.setRuntimeModule(runtime));
        }

        @SuppressWarnings("unchecked")
        @Override
        public C setDefaults() {
            return newInstance((T) delegate.setDefaults());
        }

        @Override
        public V build() {
            return setDefaults().doBuild();
        }

        protected abstract C newInstance(T delegate);
        
        protected abstract V doBuild();
    }
    
    public static class ForwardingApplication extends ZooKeeperApplication {

        protected final Application delegate;
        
        public ForwardingApplication(Application delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public void run() {
            delegate.run();
        }
    }
    
    @Configurable(arg="timeout", key="timeout", value="30 seconds", help="time")
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
}
