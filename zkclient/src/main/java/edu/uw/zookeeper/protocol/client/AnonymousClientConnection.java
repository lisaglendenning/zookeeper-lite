package edu.uw.zookeeper.protocol.client;

import edu.uw.zookeeper.common.Factory;
import edu.uw.zookeeper.common.ParameterizedFactory;
import edu.uw.zookeeper.net.ClientConnectionFactory;
import edu.uw.zookeeper.net.Codec;
import edu.uw.zookeeper.net.CodecConnection;
import edu.uw.zookeeper.net.NetClientModule;
import edu.uw.zookeeper.protocol.ForwardingCodecConnection;
import edu.uw.zookeeper.protocol.Message;

public class AnonymousClientConnection<V extends Codec<? super Message.ClientAnonymous, ? extends Message.ServerAnonymous,?,?>, T extends CodecConnection<? super Message.ClientAnonymous, ? extends Message.ServerAnonymous, V, ?>> extends ForwardingCodecConnection<Message.ClientAnonymous,Message.ServerAnonymous,V,T,AnonymousClientConnection<V,T>> {

    public static ClientConnectionFactory<? extends AnonymousClientConnection<?,?>> defaults(
            NetClientModule clientModule) {
        return clientModule.getClientConnectionFactory(
                new Factory<AnonymousClientCodec>() {
                    @Override
                    public AnonymousClientCodec get() {
                        return AnonymousClientCodec.getInstance();
                    }
                }, 
                AnonymousClientConnection.<AnonymousClientCodec,CodecConnection<Message.ClientAnonymous,Message.ServerAnonymous,AnonymousClientCodec,?>>factory()).get();
    }
    
    public static <V extends Codec<? super Message.ClientAnonymous, ? extends Message.ServerAnonymous,?,?>, T extends CodecConnection<? super Message.ClientAnonymous, ? extends Message.ServerAnonymous, V, ?>> ParameterizedFactory<T, AnonymousClientConnection<V,T>> factory() {
        return new ParameterizedFactory<T, AnonymousClientConnection<V,T>>() {
                    @Override
                    public AnonymousClientConnection<V,T> get(T value) {
                        return AnonymousClientConnection.<V,T>newInstance(
                                value);
                    }
                };
    }
    
    public static <V extends Codec<? super Message.ClientAnonymous, ? extends Message.ServerAnonymous,?,?>, T extends CodecConnection<? super Message.ClientAnonymous, ? extends Message.ServerAnonymous, V, ?>> AnonymousClientConnection<V,T> newInstance(
            T connection) {
        return new AnonymousClientConnection<V,T>(connection);
    }

    protected final T connection;
    
    protected AnonymousClientConnection(
            T connection) {
        this.connection = connection;
    }

    @Override
    protected T delegate() {
        return connection;
    }
}
