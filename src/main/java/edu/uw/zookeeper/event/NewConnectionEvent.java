package edu.uw.zookeeper.event;

import com.google.common.base.Objects;

import edu.uw.zookeeper.net.Connection;

public class NewConnectionEvent implements ConnectionEvent {

    public static NewConnectionEvent create(Connection connection) {
        return new NewConnectionEvent(connection);
    }
    
    private final Connection connection;
    
    private NewConnectionEvent(Connection connection) {
        this.connection = connection;
    }
    
    @Override
    public Connection connection() {
        return connection;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("connection", connection())
                .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(connection());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        NewConnectionEvent other = (NewConnectionEvent) obj;
        return Objects.equal(connection(), other.connection());
    }
}
