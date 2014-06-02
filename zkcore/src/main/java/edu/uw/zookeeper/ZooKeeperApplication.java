package edu.uw.zookeeper;


import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import edu.uw.zookeeper.common.Application;
import edu.uw.zookeeper.common.Builder;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.common.RuntimeModule;

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
    
    public static abstract class AbstractRuntimeBuilder<T, C extends AbstractRuntimeBuilder<T,C>> implements RuntimeBuilder<T,C> {

        protected final RuntimeModule runtime;
        
        protected AbstractRuntimeBuilder(
                RuntimeModule runtime) {
            this.runtime = runtime;
        }

        @Override
        public RuntimeModule getRuntimeModule() {
            return runtime;
        }

        @Override
        public C setRuntimeModule(RuntimeModule runtime) {
            return newInstance(runtime);
        }

        @SuppressWarnings("unchecked")
        @Override
        public C setDefaults() {
            checkState(getRuntimeModule() != null);
            return (C) this;
        }

        @Override
        public T build() {
            return setDefaults().doBuild();
        }

        protected abstract C newInstance(RuntimeModule runtime);

        protected abstract T doBuild();
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
            this.delegate = checkNotNull(delegate);
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
    
    public static class ForwardingApplication implements Application {

        protected final Runnable delegate;
        
        public ForwardingApplication(Runnable delegate) {
            this.delegate = delegate;
        }
        
        @Override
        public void run() {
            delegate.run();
        }
    }
    
    protected ZooKeeperApplication() {}
}
