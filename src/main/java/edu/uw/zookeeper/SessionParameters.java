package edu.uw.zookeeper;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.proto.ConnectResponse;

import com.google.common.base.Objects;

import edu.uw.zookeeper.data.OpCreateSessionAction;
import edu.uw.zookeeper.util.TimeValue;

public class SessionParameters {

    public static final long NEVER_TIMEOUT = 0;
    public static final byte[] NO_PASSWORD = new byte[0];
    public static final int PASSWORD_LENGTH = 16;
    public static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    protected final TimeValue timeOut;
    protected final byte[] password;

    public static SessionParameters create() {
        return new SessionParameters();
    }

    public static SessionParameters create(
            OpCreateSessionAction.Response message) {
        ConnectResponse response = message.record();
        return new SessionParameters(response.getTimeOut(),
                response.getPasswd());
    }

    public static SessionParameters create(long timeOut) {
        return new SessionParameters(timeOut);
    }

    public static SessionParameters create(long timeOut, byte[] passwd) {
        return new SessionParameters(timeOut, passwd);
    }

    public static SessionParameters create(TimeValue timeOut, byte[] passwd) {
        return new SessionParameters(timeOut, passwd);
    }

    public SessionParameters() {
        this(NEVER_TIMEOUT);
    }

    public SessionParameters(long timeOut) {
        this(timeOut, NO_PASSWORD);
    }

    public SessionParameters(long timeOut, byte[] passwd) {
        this(TimeValue.create(timeOut, TIMEOUT_UNIT), passwd);
    }

    public SessionParameters(TimeValue timeOut, byte[] passwd) {
        super();
        this.timeOut = timeOut;
        this.password = passwd;
    }

    public TimeValue timeOut() {
        return timeOut;
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
            // just output a hash
            passwordStr = String.format("0x%08X", password.hashCode());
            // BigInteger bi = new BigInteger(1, password);
            // passwordStr = String.format("0x%0" + (password.length << 1) +
            // "X", bi);
        }
        return Objects.toStringHelper(this).add("password", passwordStr)
                .add("timeOut", timeOut())
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
        return Arrays.equals(password(), other.password())
                && Objects.equal(timeOut(), other.timeOut());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(password(), timeOut());
    }
}
