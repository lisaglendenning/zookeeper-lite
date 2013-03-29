package org.apache.zookeeper.server;

import org.apache.zookeeper.protocol.netty.LocalModule;
import org.apache.zookeeper.protocol.netty.server.ChannelServerConnectionGroup;
import org.apache.zookeeper.util.EventfulEventBus;
import org.apache.zookeeper.util.SettableConfiguration;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;

public class SessionConnectionManagerTest {

    @Rule
    public Timeout globalTimeout = new Timeout(10000); 

    protected final Logger logger = LoggerFactory.getLogger(SessionConnectionManagerTest.class);

    public static class Module extends LocalModule {

        public static Injector injector;
        
        public static void createInjector() {
            injector = Guice.createInjector(
                    SettableConfiguration.ConfigurationModule.get(),
                    EventfulEventBus.EventfulModule.get(),
                    DefaultSessionParametersPolicy.SessionParametersPolicyModule.get(),
                    ExpiringSessionManager.SessionManagerModule.get(),
                    Module.get());
        }

        public static Module get() {
            return new Module();
        }
        
        @Override
        protected void configure() {
            super.configure();
            bind(ServerConnectionGroup.class).to(ChannelServerConnectionGroup.class);
        }
    }
    
    public static class CallbackSink<T> implements FutureCallback<T> {

        public T result;
        
        @Override
        public void onFailure(Throwable t) {
            throw new AssertionError(t);
        }

        @Override
        public void onSuccess(T result) {
            this.result = result;
        }
    }

    @BeforeClass
    public static void createInjector() {
        Module.createInjector();
    }
    
    @Test
    public void testConnect() throws Exception {
        Injector injector = Module.injector;
        ExpiringSessionManager sessions = injector.getInstance(ExpiringSessionManager.class);
        RequestManager requests = injector.getInstance(RequestManager.class);
        SessionConnectionManager manager = injector.getInstance(SessionConnectionManager.class);
    }
    
}
