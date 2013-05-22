package edu.uw.zookeeper;


import com.google.common.base.Throwables;

import edu.uw.zookeeper.data.Serializes;
import edu.uw.zookeeper.data.Serializers;
import edu.uw.zookeeper.util.Reference;

public abstract class ServerAddressView {

    public static enum Type implements Reference<Class<? extends ServerView.Address<?>>> {
        DEFAULT(ServerAddressView.class.getName(), ServerInetAddressView.class);
        
        private final Class<? extends ServerView.Address<?>> type;
        
        @SuppressWarnings("unchecked")
        private Type(String property, Class<? extends ServerView.Address<?>> defaultType) {
            String className = System.getProperty(property);
            if (className == null) {
                this.type = defaultType;
            } else {
                try {
                    this.type = (Class<? extends ServerView.Address<?>>) Class.forName(className);
                } catch (ClassNotFoundException e) {
                    throw Throwables.propagate(e);
                }
            }                  
        }
        
        @Override
        public Class<? extends ServerView.Address<?>> get() {
            return type;
        }
    }
    
    public static Class<? extends ServerView.Address<?>> getDefaultType() {
        return Type.DEFAULT.get();
    }

    @Serializes(from=ServerView.Address.class, to=String.class)
    public static String toString(ServerView.Address<?> input) {
        return Serializers.getInstance().toClass(input, String.class);
    }

    @Serializes(from=String.class, to=ServerView.Address.class)
    public static ServerView.Address<?> fromString(String input) {
        return Serializers.getInstance().toClass(input, getDefaultType());
    }
    
    private ServerAddressView() {}
}
