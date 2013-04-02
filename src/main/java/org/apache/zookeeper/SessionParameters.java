package org.apache.zookeeper;

import java.math.BigInteger;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.proto.ConnectResponse;
import org.apache.zookeeper.protocol.OpCreateSessionAction;

import com.google.common.base.Objects;

public class SessionParameters {
    
    public static final long NEVER_TIMEOUT = 0;
    public static final byte[] NO_PASSWORD = new byte[0];
    public static final int PASSWORD_LENGTH = 16;
    public static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
    
    protected final long timeOut;
    protected final TimeUnit timeOutUnit;
    protected final byte[] password;
    
    public static SessionParameters create() {
        return new SessionParameters();
    }
    
    public static SessionParameters create(OpCreateSessionAction.Response message) {
        ConnectResponse response = message.record();
        return new SessionParameters(response.getTimeOut(), response.getPasswd());
    }
    
    public static SessionParameters create(long timeOut) {
        return new SessionParameters(timeOut);
    }

    public static SessionParameters create(long timeOut, byte[] passwd) {
        return new SessionParameters(timeOut, passwd);
    }

    public static SessionParameters create(long timeOut, byte[] passwd, TimeUnit timeOutUnit) {
        return new SessionParameters(timeOut, passwd, timeOutUnit);
    }
    
    public SessionParameters() {
        this(NEVER_TIMEOUT);
    }

    public SessionParameters(long timeOut) {
        this(timeOut, NO_PASSWORD);
    }

    public SessionParameters(long timeOut, byte[] passwd) {
        this(timeOut, passwd, TIMEOUT_UNIT);
    }
    
    public SessionParameters(long timeOut, byte[] passwd, TimeUnit timeOutUnit) {
        super();
        this.timeOut = timeOut;
        this.password = passwd;
        this.timeOutUnit = timeOutUnit;
    }

    public long timeOut() {
        return timeOut;
    }

    public TimeUnit timeOutUnit() {
        return timeOutUnit;
    }

    public byte[] password() {
        return password;
    }
    
    @Override
    public String toString() {
        byte[] password = password();
        String passwordStr;
        if (password == null) {
            passwordStr = "null";
        } else if (password.length == 0) {
            passwordStr = "";
        } else {
            BigInteger bi = new BigInteger(1, password);
            passwordStr = String.format("%0" + (password.length << 1) + "X", bi);
        }
        return Objects.toStringHelper(this)
                .add("password", passwordStr)
                .add("timeOut", timeOut())
                .add("timeOutUnit", timeOutUnit())
                .toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SessionParameters other = (SessionParameters) obj;
        return Objects.equal(password(), other.password()) 
                && Objects.equal(timeOut(), other.timeOut())
                && Objects.equal(timeOutUnit(), other.timeOutUnit());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(password(), timeOut(), timeOutUnit());
    }
}
