package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.*;

import java.net.SocketAddress;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import edu.uw.zookeeper.util.Singleton;

public class ServerNetView<T extends SocketAddress> implements Singleton<T>, ServerView {

    private final static String PROPERTY_TYPE = ServerNetView.class.getName();
    
    @SuppressWarnings("unchecked")
    public static Class<? extends ServerNetView<?>> getDefaultType() throws ClassNotFoundException {
        String className = System.getProperty(PROPERTY_TYPE);
        if (className == null) {
            return ServerInetView.class;
        } else {
            return (Class<? extends ServerNetView<?>>) Class.forName(className);
        }
    }
    
    public static ServerNetView<?> newInstance(Object...args) throws ClassNotFoundException {
        Class<? extends ServerNetView<?>> type = getDefaultType();
        try {
            return (ServerNetView<?>) type.getMethod("newInstance").invoke(args);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }
    
    public static <T extends SocketAddress> String toString(ServerNetView<T> input) {
        @SuppressWarnings("rawtypes")
        Class<? extends ServerNetView> type = input.getClass();
        String output;
        try {
            output = (String) type.getMethod("toString", type).invoke(input);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return output;
    }

    public static ServerNetView<?> fromString(String input) throws ClassNotFoundException {
        return fromString(input, getDefaultType());
    }
    
    public static ServerNetView<?> fromString(String input, Class<? extends ServerNetView<?>> netViewClass) {
        ServerNetView<?> view;
        try {
            view = (ServerNetView<?>) netViewClass.getMethod("fromString", String.class).invoke(null, input);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        return view;
    }
    
    private final T address;

    protected ServerNetView(T address) {
        this.address = checkNotNull(address);
    }

    @Override
    public T get() {
        return address;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("address", get()).toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(get());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ServerNetView<?> other = (ServerNetView<?>) obj;
        return Objects.equal(get(), other.get());
    }
}
