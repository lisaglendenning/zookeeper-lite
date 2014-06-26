package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.*;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;

import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;

import edu.uw.zookeeper.data.Serializes;

public class ServerInetAddressView implements Supplier<InetSocketAddress>, Comparable<ServerInetAddressView> {

    public static abstract class Address {

        public static final char TOKEN_SEP = ':';
        private static final Splitter SPLITTER = Splitter.on(TOKEN_SEP).trimResults().limit(2);
        
        /**
         * Convert an InetSocketAddress to a String of the form "Address:Port".
         * 
         * @param address InetSocketAddress to represent
         * @return "Address:Port"
         */
        @Serializes
        public static String toString(InetSocketAddress address) {
            checkNotNull(address);
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

        /**
         * Convert a String the form "Address:Port" to an InetSocketAddress.
         * 
         * A port of 0 directs the system to pick an ephemeral port.
         * An empty string is interpreted as ":0".
         * An empty address is interpreted as the wildcard address.
         * An address string can be either a hostname or an IP address.
         * 
         * @param input "Address:Port"
         * @return InetSocketAddress representing input
         * @throws UnknownHostException 
         */
        @Serializes
        public static InetSocketAddress fromString(String input) throws UnknownHostException {
            checkNotNull(input);
            String[] fields = Iterables.toArray(SPLITTER.split(input), String.class);
            InetSocketAddress address;
            if (fields.length == 0) {
                address = new InetSocketAddress(0);
            } else {
                String portField = (fields.length > 1) ? fields[1] : fields[0];
                int port = Integer.parseInt(portField);
                if (fields.length == 1) {
                    address = new InetSocketAddress(port);
                } else {
                    String field = fields[0];
                    if (field.equals("*")) {
                        address = new InetSocketAddress(port);
                    } else if (field.equals("localhost")) {
                        address = new InetSocketAddress(InetAddress.getLocalHost(), port);
                    } else {
                        address = new InetSocketAddress(InetAddress.getByName(field), port);
                    }
                }
            }        
            return address;
        }

        private Address() {}
    }

    @Serializes
    public static ServerInetAddressView fromString(String input)
            throws UnknownHostException {
        return of(Address.fromString(input));
    }

    @Serializes(to=EnsembleView.class)
    public static EnsembleView<ServerInetAddressView> ensembleFromString(String input) {
        return EnsembleView.copyOf(EnsembleView.fromString(input, ServerInetAddressView.class));
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
    
    private InetSocketAddress value;
    
    protected ServerInetAddressView(InetSocketAddress value) {
        this.value = value;
    }
    
    @Override
    public InetSocketAddress get() {
        return value;
    }

    @Override
    public int hashCode() {
        return get().hashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ServerInetAddressView) {
            return get().equals(((ServerInetAddressView) obj).get());
        }
        return false;
    }

    @Serializes
    @Override
    public String toString() {
        return Address.toString(get());
    }

    @Override
    public int compareTo(ServerInetAddressView other) {
        return toString().compareTo(other.toString());
    }
}
