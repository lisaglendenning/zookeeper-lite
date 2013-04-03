package org.apache.zookeeper.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper.Session;
import org.apache.zookeeper.SessionParameters;
import org.apache.zookeeper.util.Configurable;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.Parameters;

import com.google.inject.Inject;

public class DefaultSessionParametersPolicy implements SessionParametersPolicy, Configurable {

    public static DefaultSessionParametersPolicy create() {
        return new DefaultSessionParametersPolicy();
    }

    public static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;
    public static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    protected static final Random RANDOM = new Random();
    protected static final long SECRET = RANDOM.nextLong();
    protected static final AtomicInteger COUNTER = new AtomicInteger(0);
    
    public final String PARAM_KEY_MIN_TIMEOUT = "Sessions.MinTimeout";
    public final long PARAM_DEFAULT_MIN_TIMEOUT = 0;
    public final Parameters.Parameter<Long> PARAM_MIN_TIMEOUT = 
            Parameters.newParameter(PARAM_KEY_MIN_TIMEOUT, PARAM_DEFAULT_MIN_TIMEOUT);

    public final String PARAM_KEY_MAX_TIMEOUT = "Sessions.MaxTimeout";
    public final long PARAM_DEFAULT_MAX_TIMEOUT = SessionParameters.NEVER_TIMEOUT;
    public final Parameters.Parameter<Long> PARAM_MAX_TIMEOUT = 
            Parameters.newParameter(PARAM_KEY_MAX_TIMEOUT, PARAM_DEFAULT_MAX_TIMEOUT);

    public final String PARAM_KEY_TIMEOUT_UNIT = "Sessions.TimeoutUnit";
    public final String PARAM_DEFAULT_TIMEOUT_UNIT = "MILLISECONDS";
    public final Parameters.Parameter<String> PARAM_TIMEOUT_UNIT = 
            Parameters.newParameter(PARAM_KEY_TIMEOUT_UNIT, PARAM_DEFAULT_TIMEOUT_UNIT);
    
    public final Parameters parameters = Parameters.newInstance()
            .add(PARAM_MIN_TIMEOUT).add(PARAM_MAX_TIMEOUT).add(PARAM_TIMEOUT_UNIT);

    @Inject
    protected DefaultSessionParametersPolicy(Configuration configuration) {
        this();
        configure(configuration);
    }
    
    protected DefaultSessionParametersPolicy() {}

    @Override
    public void configure(Configuration configuration) {
        parameters.configure(configuration);
        if (PARAM_MAX_TIMEOUT.getValue() != SessionParameters.NEVER_TIMEOUT) {
            checkArgument(PARAM_MIN_TIMEOUT.getValue() <= PARAM_MAX_TIMEOUT.getValue());
        }
        checkArgument(TimeUnit.valueOf(PARAM_TIMEOUT_UNIT.getValue()) != null);
    }

    @Override
    public byte[] newPassword(long seed) {
        Random r = new Random(seed ^ SECRET);
        byte p[] = new byte[SessionParameters.PASSWORD_LENGTH];
        r.nextBytes(p);
        return p;
    }

    @Override
    public boolean validatePassword(long sessionId, byte[] passwd) {
        return sessionId != Session.UNINITIALIZED_ID
                && Arrays.equals(passwd, newPassword(sessionId));
    }

    @Override
    public synchronized long newSessionId() {
        // synchronized since unsure that Random is thread-safe
        // TODO: add some other non-time-based seed
        // to avoid collision with other servers
        // ideally, seed would look like:
        // sever-id | nonce
        // so that we can map a session to a server easily
        int count = COUNTER.incrementAndGet();
        int nonce = RANDOM.nextInt();
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.order(BYTE_ORDER);
        bb.putInt(count);
        bb.putInt(nonce);
        bb.flip();
        long sessionId = bb.getLong();
        return sessionId;
    }

    @Override
    public long boundTimeout(long timeOut, TimeUnit unit) {
        TimeUnit timeoutUnit = timeoutUnit();
        long maxTimeout = unit.convert(maxTimeout(), timeoutUnit);
        if (maxTimeout != SessionParameters.NEVER_TIMEOUT && timeOut > maxTimeout) {
            timeOut = maxTimeout;
        } else {
            long minTimeout = unit.convert(minTimeout(), timeoutUnit);
            if (timeOut < minTimeout) {
                timeOut = minTimeout;
            }
        }
        return timeOut;
    }

    @Override
    public long maxTimeout() {
        return PARAM_MAX_TIMEOUT.getValue();
    }

    @Override
    public long minTimeout() {
        return PARAM_MIN_TIMEOUT.getValue();
    }
    
    @Override
    public TimeUnit timeoutUnit() {
        return TimeUnit.valueOf(PARAM_TIMEOUT_UNIT.getValue());
    }
}