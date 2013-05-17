package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import edu.uw.zookeeper.util.Factories;

public class ServerInetAddressView extends Factories.HolderFactory<InetSocketAddress> implements ServerView, ServerView.Address<InetSocketAddress> {

    public static final char TOKEN_SEP = ':';

    public static String toString(ServerInetAddressView input) {
        InetSocketAddress address = checkNotNull(input).get();
        return String.format("%s%c%d", address.getHostName(), TOKEN_SEP, address.getPort());
    }

    public static ServerInetAddressView fromString(String input)
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
    public static ServerInetAddressView newInstance() {
        return newInstance(0);
    }

    /**
     * Wild-card address.
     */
    public static ServerInetAddressView newInstance(int port) {
        return newInstance(new InetSocketAddress(port));
    }

    /**
     * Zero-length hostname is interpreted as the wild-card address.
     */
    public static ServerInetAddressView newInstance(String host, int port) {
        if (checkNotNull(host).length() == 0) {
            return newInstance(port);
        } else {
            return newInstance(new InetSocketAddress(host, port));
        }
    }

    public static ServerInetAddressView newInstance(InetAddress addr, int port) {
        return newInstance(new InetSocketAddress(addr, port));
    }

    public static ServerInetAddressView newInstance(InetSocketAddress address) {
        return new ServerInetAddressView(address);
    }
    
    private ServerInetAddressView(InetSocketAddress address) {
        super(address);
    }
}
