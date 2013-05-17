package edu.uw.zookeeper;

import java.net.SocketAddress;

import com.google.common.base.Throwables;

public abstract class ServerAddressView {

    private final static String PROPERTY_SERVER_ADDRESS_VIEW_TYPE = ServerAddressView.class.getName();
    
    @SuppressWarnings("unchecked")
    public static Class<? extends ServerView.Address<?>> getDefaultType() throws ClassNotFoundException {
        String className = System.getProperty(PROPERTY_SERVER_ADDRESS_VIEW_TYPE);
        if (className == null) {
            return ServerInetAddressView.class;
        } else {
            return (Class<? extends ServerView.Address<?>>) Class.forName(className);
        }
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends SocketAddress> ServerView.Address<T> newInstance(Object...args) throws ClassNotFoundException {
        Class<? extends ServerView.Address<?>> type = getDefaultType();
        try {
            return (ServerView.Address<T>) type.getMethod("newInstance").invoke(args);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
    
    public static String toString(ServerView.Address<?> input) {
        @SuppressWarnings("rawtypes")
        Class<? extends ServerView.Address> type = input.getClass();
        String output;
        try {
            output = (String) type.getMethod("toString", type).invoke(input);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return output;
    }

    public static ServerView.Address<?> fromString(String input) throws ClassNotFoundException {
        return fromString(input, getDefaultType());
    }
    
    public static ServerView.Address<?> fromString(String input, Class<? extends ServerView.Address<?>> type) {
        ServerView.Address<?> view;
        try {
            view = (ServerView.Address<?>) type.getMethod("fromString", String.class).invoke(null, input);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return view;
    }
    
    private ServerAddressView() {}
}
