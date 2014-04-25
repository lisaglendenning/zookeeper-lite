package edu.uw.zookeeper.server;

import static com.google.common.base.Preconditions.checkArgument;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

import edu.uw.zookeeper.ZooKeeperApplication;
import edu.uw.zookeeper.common.Configurable;
import edu.uw.zookeeper.common.Configuration;
import edu.uw.zookeeper.common.TimeValue;
import edu.uw.zookeeper.protocol.Session;

public class DefaultSessionParametersPolicy implements SessionParametersPolicy {

    public static DefaultSessionParametersPolicy fromConfiguration(short id, Configuration configuration) {
        TimeValue minTimeout = ConfigurableMinTimeout.get(configuration);
        TimeValue maxTimeout = ConfigurableMaxTimeout.get(configuration);
        if (maxTimeout.value() != Session.Parameters.NEVER_TIMEOUT) {
            checkArgument(minTimeout.value() <= maxTimeout.value());
        }
        return defaults(id, minTimeout, maxTimeout);
    }

    @Configurable(path="sessions", key="minTimeout", value="0 seconds", help="time")
    public static class ConfigurableMinTimeout extends ZooKeeperApplication.ConfigurableTimeout {
        
        public static TimeValue get(Configuration configuration) {
            return new ConfigurableMinTimeout().apply(configuration);
        }
    }

    @Configurable(path="sessions", key="maxTimeout", value="0 seconds", help="time")
    public static class ConfigurableMaxTimeout extends ZooKeeperApplication.ConfigurableTimeout {

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
        return new DefaultSessionParametersPolicy(
                id, new AtomicInteger(counter), secret, random, minTimeout, maxTimeout);
    }

    protected static final ByteOrder BYTE_ORDER = ByteOrder.BIG_ENDIAN;

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
            TimeValue maxTimeout) {
        this.random = random;
        this.id = id;
        this.secret = secret;
        this.counter = counter;
        this.minTimeout = minTimeout;
        this.maxTimeout = maxTimeout;
    }

    @Override
    public byte[] newPassword(long seed) {
        Random r = new Random(seed ^ secret);
        byte p[] = new byte[Session.Parameters.PASSWORD_LENGTH];
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
        // TODO: add some other non-time-based seed
        // to avoid collision with other servers
        // ideally, seed would look like:
        // sever-id | nonce
        // so that we can map a session to a server easily
        int count = counter.incrementAndGet();
        short nonce = (short) random.nextInt();
        ByteBuffer bb = ByteBuffer.allocate(8);
        bb.order(BYTE_ORDER);
        bb.putShort(id);
        bb.putInt(count);
        bb.putInt(nonce);
        bb.flip();
        long sessionId = bb.getLong();
        return sessionId;
    }

    @Override
    public TimeValue boundTimeout(TimeValue timeOut) {
        TimeValue maxTimeout = maxTimeout();
        if ((maxTimeout.value() != Session.Parameters.NEVER_TIMEOUT)
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
}