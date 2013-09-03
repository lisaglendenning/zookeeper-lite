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

    public static <T extends RuntimeBuilder<? extends Application>> void main(String[] args, T builder) {
        DefaultMain.main(args, factory(builder));
    }
 
    public static <T extends RuntimeBuilder<? extends Application>> ApplicationFactory<T> factory(T builder) {
        return ApplicationFactory.create(builder);
    }
    
    public static interface RuntimeBuilder<T> extends Builder<T> {
        
        RuntimeModule getRuntimeModule();
        
        RuntimeBuilder<T> setRuntimeModule(RuntimeModule runtime);
    }

    public static class ApplicationFactory<T extends RuntimeBuilder<? extends Application>> implements ParameterizedFactory<RuntimeModule, Application> {

        public static <T extends RuntimeBuilder<? extends Application>> ApplicationFactory<T> create(T builder) {
            return new ApplicationFactory<T>(builder);
        }
        
        protected final T builder;
        
        public ApplicationFactory(T builder) {
            this.builder = builder;
        }
        
        @Override
        public Application get(RuntimeModule runtime) {
            return builder.setRuntimeModule(runtime).build();
        }
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
}
