package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Objects;

import edu.uw.zookeeper.ConfigurableTimeout;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.Hex;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Session;

public class DefaultSessionParametersPolicy implements SessionParametersPolicy {

    public static DefaultSessionParametersPolicy fromConfiguration(short id, Configuration configuration) {
        TimeValue minTimeout = ConfigurableMinTimeout.get(configuration);
        TimeValue maxTimeout = ConfigurableMaxTimeout.get(configuration);
        if (maxTimeout.value() != Session.Parameters.noTimeout()) {
            checkArgument(minTimeout.value() <= maxTimeout.value());
        }
        return defaults(id, minTimeout, maxTimeout);
    }

    @Configurable(path="sessions", key="minTimeout", value="0 seconds", help="time")
    public static class ConfigurableMinTimeout extends ConfigurableTimeout {
        
        public static TimeValue get(Configuration configuration) {
            return new ConfigurableMinTimeout().apply(configuration);
        }
    }

    @Configurable(path="sessions", key="maxTimeout", value="0 seconds", help="time")
    public static class ConfigurableMaxTimeout extends ConfigurableTimeout {

        public static TimeValue get(Configuration configuration) {
            return new ConfigurableMaxTimeout().apply(configuration);
        }
    }

    public static DefaultSessionParametersPolicy defaults(
            short id, 
            TimeValue minTimeout, 
            TimeValue maxTimeout) {
        Random random = new Random();
        return create(id, 0, random.nextLong(), random, minTimeout, maxTimeout);
    }
            
    public static DefaultSessionParametersPolicy create(
            short id, 
            int counter,
            long secret,
            Random random,
            TimeValue minTimeout, 
            TimeValue maxTimeout) {
        DefaultSessionParametersPolicy instance = new DefaultSessionParametersPolicy(
                id, new AtomicInteger(counter), secret, random, minTimeout, maxTimeout, LogManager.getLogger(DefaultSessionParametersPolicy.class));
        return instance;
    }

    protected static final int PASSWORD_LENGTH = 16;
    
    protected final Logger logger;
    protected final Random random;
    protected final long secret;
    protected final short id;
    protected final AtomicInteger counter;
    protected final TimeValue minTimeout;
    protected final TimeValue maxTimeout;
    
    protected DefaultSessionParametersPolicy(
            short id, 
            AtomicInteger counter,
            long secret,
            Random random,
            TimeValue minTimeout, 
            TimeValue maxTimeout,
            Logger logger) {
        this.random = random;
        this.id = id;
        this.secret = secret;
        this.counter = counter;
        this.minTimeout = minTimeout;
        this.maxTimeout = maxTimeout;
        this.logger = logger;
        
        logger.info("{}", this);
    }

    @Override
    public byte[] newPassword(long seed) {
        Random r = new Random(seed ^ secret);
        byte p[] = new byte[PASSWORD_LENGTH];
        r.nextBytes(p);
        return p;
    }

    @Override
    public boolean validatePassword(long sessionId, byte[] passwd) {
        return (sessionId != Session.uninitialized().id())
                && Arrays.equals(passwd, newPassword(sessionId));
    }

    @Override
    public long newSessionId() {
        int count = counter.incrementAndGet();
        byte[] nonce = new byte[2];
        random.nextBytes(nonce);
        ByteBuffer bb = ByteBuffer.allocate(8).order(ByteOrder.BIG_ENDIAN);
        bb.putShort(id).put(nonce).putInt(count).flip();
        return bb.getLong();
    }

    @Override
    public TimeValue boundTimeout(TimeValue timeOut) {
        TimeValue maxTimeout = maxTimeout();
        if ((maxTimeout.value() != Session.Parameters.noTimeout())
                && (maxTimeout.value() < timeOut.value(maxTimeout.unit()))) {
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
        return maxTimeout;
    }

    @Override
    public TimeValue minTimeout() {
        return minTimeout;
    }
    
    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", String.format("0x%s", Hex.toPaddedHexString(id))).add("secret", String.format("0x%8s", Long.toHexString(secret)).replace(' ', '0')).add("counter", counter).add("minTimeout", minTimeout).add("maxTimeout", maxTimeout).toString();
    }
}