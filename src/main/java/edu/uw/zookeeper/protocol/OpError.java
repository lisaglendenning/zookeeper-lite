package edu.uw.zookeeper.protocol;

import org.apache.zookeeper.KeeperException;

import com.google.common.base.Objects;


public class OpError implements Operation.Error {

    public static OpError create(KeeperException.Code error) {
        return new OpError(error);
    }

    private final KeeperException.Code error;
    
    private OpError(KeeperException.Code error) {
        this.error = error;
    }

    @Override
    public KeeperException.Code error() {
        return error;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("error", error()).toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        OpError other = (OpError) obj;
        return Objects.equal(error(), other.error());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(error());
    }
}
