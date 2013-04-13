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
import org.apache.zookeeper.util.ConfigurableTime;
import org.apache.zookeeper.util.Configuration;
import org.apache.zookeeper.util.TimeValue;

import com.google.inject.Inject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigUtil;

public class DefaultSessionParametersPolicy implements SessionParametersPolicy,
        Configurable {

    public static DefaultSessionParametersPolicy create() {
        return new DefaultSessionParametersPolicy();
    }

    public static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;
    public static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;

    protected static final Random RANDOM = new Random();
    protected static final long SECRET = RANDOM.nextLong();
    protected static final AtomicInteger COUNTER = new AtomicInteger(0);

    public static final String CONFIG_PATH = "Sessions.Policy";
    
    public final String KEY_MIN_TIMEOUT = "MinTimeout";
    public final long DEFAULT_MIN_TIMEOUT = 0;

    public final String KEY_MAX_TIMEOUT = "MaxTimeout";
    public final long DEFAULT_MAX_TIMEOUT = SessionParameters.NEVER_TIMEOUT;

    public final String DEFAULT_TIMEOUT_UNIT = "MILLISECONDS";

    protected final ConfigurableTime minTimeout;
    protected final ConfigurableTime maxTimeout;
    
    @Inject
    protected DefaultSessionParametersPolicy(Configuration configuration) {
        this();
        configure(configuration);
    }

    protected DefaultSessionParametersPolicy() {
        this.minTimeout = ConfigurableTime.create(
                DEFAULT_MIN_TIMEOUT,
                DEFAULT_TIMEOUT_UNIT);
        this.maxTimeout = ConfigurableTime.create(
                DEFAULT_MAX_TIMEOUT,
                DEFAULT_TIMEOUT_UNIT);
    }

    @Override
    public void configure(Configuration configuration) {
        try {
            Config config = configuration.get().getConfig(
                    ConfigUtil.joinPath(CONFIG_PATH, KEY_MIN_TIMEOUT));
            minTimeout.get(config);
        } catch (ConfigException.Missing e) {}
        
        try {
            Config config = configuration.get().getConfig(
                    ConfigUtil.joinPath(CONFIG_PATH, KEY_MAX_TIMEOUT)); 
            maxTimeout.get(config);
        } catch (ConfigException.Missing e) {}
        
        if (maxTimeout().value() != SessionParameters.NEVER_TIMEOUT) {
            checkArgument(minTimeout().value() <= maxTimeout().value());
        }
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
    public TimeValue boundTimeout(TimeValue timeOut) {
        TimeValue maxTimeout = maxTimeout();
        if (maxTimeout.value() != SessionParameters.NEVER_TIMEOUT
                && maxTimeout.value() < timeOut.value(maxTimeout.unit())) {
            timeOut = maxTimeout;
        } else {
            TimeValue minTimeout = minTimeout();
            if (minTimeout.value() > timeOut.value(minTimeout.unit())) {
                timeOut = minTimeout;
            }
        }
        return timeOut;
    }

    @Override
    public TimeValue maxTimeout() {
        return maxTimeout.get();
    }

    @Override
    public TimeValue minTimeout() {
        return minTimeout.get();
    }
}