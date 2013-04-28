package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

public class ServerInetView extends ServerNetView<InetSocketAddress> {

    public static final char TOKEN_SEP = ':';

    public static String toString(ServerInetView input) {
        InetSocketAddress address = checkNotNull(input).get();
        return String.format("%s%c%d", address.getHostName(), TOKEN_SEP, address.getPort());
    }

    public static ServerInetView fromString(String input)
            throws IllegalArgumentException {
        checkNotNull(input);
        Splitter splitter = Splitter.on(TOKEN_SEP).trimResults().limit(2);

        String[] fields = Iterables.toArray(splitter.split(input), String.class);
        if (fields.length == 0) {
            return newInstance();
        } else {
            String portField = (fields.length > 1) ? fields[1] : fields[0];
            int port = Integer.parseInt(portField);
            String host = (fields.length > 1) ? fields[0] : "";
            return newInstance(host, port);
        }
    }

    /**
     * Wild-card address and ephemeral port.
     */
    public static ServerInetView newInstance() {
        return newInstance(0);
    }

    /**
     * Wild-card address.
     */
    public static ServerInetView newInstance(int port) {
        return newInstance(new InetSocketAddress(port));
    }

    /**
     * Zero-length hostname is interpreted as the wild-card address.
     */
    public static ServerInetView newInstance(String host, int port) {
        if (checkNotNull(host).length() == 0) {
            return newInstance(port);
        } else {
            return newInstance(new InetSocketAddress(host, port));
        }
    }

    public static ServerInetView newInstance(InetAddress addr, int port) {
        return newInstance(new InetSocketAddress(addr, port));
    }

    public static ServerInetView newInstance(InetSocketAddress address) {
        return new ServerInetView(address);
    }
    
    public ServerInetView(InetSocketAddress address) {
        super(address);
    }
}
