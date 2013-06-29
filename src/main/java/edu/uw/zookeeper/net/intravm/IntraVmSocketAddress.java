package edu.uw.zookeeper.net.intravm;

import java.net.SocketAddress;

import com.google.common.base.Objects;

import edu.uw.zookeeper.util.Reference;

public class IntraVmSocketAddress extends SocketAddress implements Reference<Object> {

    public static IntraVmSocketAddress of(Object obj) {
        return new IntraVmSocketAddress(obj);
    }
    
    private static final long serialVersionUID = -5202987224082444644L;

    private final Object obj;
    
    public IntraVmSocketAddress(Object obj) {
        super();
        this.obj = obj;
    }

    @Override
    public Object get() {
        return obj;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).addValue(get()).toString();
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
        if (! (obj instanceof IntraVmSocketAddress)) {
            return false;
        }
        IntraVmSocketAddress other = (IntraVmSocketAddress) obj;
        return Objects.equal(get(), other.get());
    }
}
