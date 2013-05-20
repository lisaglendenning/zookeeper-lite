package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import com.google.common.base.Objects;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import edu.uw.zookeeper.data.Serializes;
import edu.uw.zookeeper.util.Factories;

public class ServerInetAddressView extends Factories.HolderFactory<InetSocketAddress> implements ServerView, ServerView.Address<InetSocketAddress> {

    public static abstract class Address {

        public static final char TOKEN_SEP = ':';
        private static final Splitter SPLITTER = Splitter.on(TOKEN_SEP).trimResults().limit(2);
        
        @Serializes(from=InetSocketAddress.class, to=String.class)
        public static String toString(InetSocketAddress address) {
            // Prefer IP address, otherwise use the hostname
            String host;
            InetAddress ip = address.getAddress();
            if (ip != null) {
                host = ip.getHostAddress();
            } else {
                host = address.getHostName();
            }
            return String.format("%s%c%d", host, TOKEN_SEP, address.getPort());
        }

        @Serializes(from=String.class, to=InetSocketAddress.class)
        public static InetSocketAddress fromString(String input) {
            checkNotNull(input);
            String[] fields = Iterables.toArray(SPLITTER.split(input), String.class);
            if (fields.length == 0) {
                return new InetSocketAddress(0);
            } else {
                String portField = (fields.length > 1) ? fields[1] : fields[0];
                int port = Integer.parseInt(portField);
                if (fields.length > 1) {
                    return new InetSocketAddress(fields[0], port);
                } else {
                    return new InetSocketAddress(port);
                }
            }        
        }

        private Address() {}
    }

    @Serializes(from=ServerInetAddressView.class, to=String.class)
    public static String toString(ServerInetAddressView input) {
        return Address.toString(checkNotNull(input).get());
    }

    @Serializes(from=String.class, to=ServerInetAddressView.class)
    public static ServerInetAddressView fromString(String input)
            throws IllegalArgumentException {
        return of(Address.fromString(input));
    }

    /**
     * Wild-card address and ephemeral port.
     */
    public static ServerInetAddressView ephemeral() {
        return unspecified(0);
    }

    /**
     * Wild-card address.
     */
    public static ServerInetAddressView unspecified(int port) {
        return of(new InetSocketAddress(port));
    }

    /**
     * Zero-length hostname is interpreted as the wild-card address.
     */
    public static ServerInetAddressView of(String host, int port) {
        if (checkNotNull(host).length() == 0) {
            return unspecified(port);
        } else {
            return of(new InetSocketAddress(host, port));
        }
    }

    public static ServerInetAddressView of(InetAddress addr, int port) {
        return of(new InetSocketAddress(addr, port));
    }

    public static ServerInetAddressView of(InetSocketAddress address) {
        return new ServerInetAddressView(address);
    }
    
    protected ServerInetAddressView(InetSocketAddress address) {
        super(address);
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
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ServerInetAddressView other = (ServerInetAddressView) obj;
        return Objects.equal(get(), other.get());
    }
}
