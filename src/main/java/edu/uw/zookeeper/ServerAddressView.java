package edu.uw.zookeeper;


import com.google.common.base.Throwables;

import edu.uw.zookeeper.data.Serializes;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.util.Reference;

public abstract class ServerAddressView {

    public static enum Type implements Reference<Class<? extends ServerView.Address<?>>> {
        DEFAULT {
            @SuppressWarnings("unchecked")
            @Override
            public Class<? extends ServerView.Address<?>> get() {
                String className = System.getProperty(PROPERTY_SERVER_ADDRESS_VIEW_TYPE);
                if (className == null) {
                    return ServerInetAddressView.class;
                } else {
                    try {
                        return (Class<? extends ServerView.Address<?>>) Class.forName(className);
                    } catch (ClassNotFoundException e) {
                        throw Throwables.propagate(e);
                    }
                }                
            }
        };
        
        private final static String PROPERTY_SERVER_ADDRESS_VIEW_TYPE = ServerAddressView.class.getName();
    }
    
    public static Class<? extends ServerView.Address<?>> getDefaultType() {
        return Type.DEFAULT.get();
    }

    @Serializes(from=ServerView.Address.class, to=String.class)
    public static String toString(ServerView.Address<?> input) {
        @SuppressWarnings("rawtypes")
        Class<? extends ServerView.Address> type = input.getClass();
        Serializers.Serializer method = Serializers.getInstance().find(type, type, String.class);
        String output;
        try {
            output = (String) method.method().invoke(input);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return output;
    }

    @Serializes(from=String.class, to=ServerView.Address.class)
    public static ServerView.Address<?> fromString(String input) {
        return fromString(input, getDefaultType());
    }
    
    public static ServerView.Address<?> fromString(String input, Class<? extends ServerView.Address<?>> type) {
        ServerView.Address<?> view;
        Serializers.Serializer method = Serializers.getInstance().find(type, String.class, type);
        try {
            view = (ServerView.Address<?>) method.method().invoke(null, input);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return view;
    }
    
    private ServerAddressView() {}
}
