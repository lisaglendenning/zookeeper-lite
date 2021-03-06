package edu.uw.zookeeper.protocol;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Function;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Hex;
import edu.uw.zookeeper.common.TimeValue;

public final class Session {

    private static final long UNINITIALIZED_ID = 0;
    private static final Session UNINITIALIZED = new Session(UNINITIALIZED_ID, Parameters.uninitialized());

    public static Session uninitialized() {
        return UNINITIALIZED;
    }

    public static Session create(long id, Parameters parameters) {
        return new Session(id, parameters);
    }
    
    public static String toString(long id) {
        return String.format("0x%s", Hex.toPaddedHexString(id));
    }
    
    private final long id;
    private final Parameters parameters;

    private Session(long id, Parameters parameters) {
        this.id = id;
        this.parameters = checkNotNull(parameters);
    }

    public long id() {
        return id;
    }

    public Parameters parameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", toString(id))
                .add("parameters", parameters).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        Session other = (Session) obj;
        return Objects.equal(id, other.id)
                && Objects.equal(parameters, other.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }

    public static enum State implements Function<State, Optional<State>> {
        SESSION_UNINITIALIZED {
            @Override
            public Optional<State> apply(State nextState) {
                switch (nextState) {
                case SESSION_UNINITIALIZED:
                    return Optional.absent();
                case SESSION_OPENED:
                    return Optional.of(nextState);
                default:
                    throw new IllegalArgumentException();
                }
            }
        },
        SESSION_OPENED {
            @Override
            public Optional<State> apply(State nextState) {
                switch (nextState) {
                case SESSION_OPENED:
                    return Optional.absent();
                case SESSION_EXPIRED:
                case SESSION_CLOSED:
                    return Optional.of(nextState);
                default:
                    throw new IllegalArgumentException();
                }
            }
        },
        SESSION_EXPIRED {
            @Override
            public Optional<State> apply(State nextState) {
                switch (nextState) {
                case SESSION_EXPIRED:
                    return Optional.absent();
                case SESSION_CLOSED:
                    return Optional.of(nextState);
                default:
                    throw new IllegalArgumentException();
                }
            }
        },
        SESSION_CLOSED {
            @Override
            public Optional<State> apply(State nextState) {
                switch (nextState) {
                case SESSION_CLOSED:
                    return Optional.absent();
                default:
                    throw new IllegalArgumentException();
                }
            }
        };
    }

    public static class Parameters {
    
        private static final long NO_TIMEOUT = 0L;
        private static final byte[] NO_PASSWORD = new byte[0];
        private static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
        private static final Parameters UNINITIALIZED_PARAMETERS = create(NO_TIMEOUT, NO_PASSWORD);
        
        public static long noTimeout() {
            return NO_TIMEOUT;
        }

        public static TimeUnit timeoutUnit() {
            return TIMEOUT_UNIT;
        }
        
        public static Parameters uninitialized() {
            return UNINITIALIZED_PARAMETERS;
        }
    
        public static Parameters create(long timeOut) {
            return create(timeOut, NO_PASSWORD);
        }
    
        public static Parameters create(TimeValue timeOut, byte[] password) {
            return new Parameters(timeOut.convert(TIMEOUT_UNIT), checkNotNull(password));
        }
    
        public static Parameters create(long timeOut, byte[] password) {
            return new Parameters(TimeValue.create(timeOut, TIMEOUT_UNIT), checkNotNull(password));
        }
    
        private final TimeValue timeOut;
        private final byte[] password;
    
        private Parameters(TimeValue timeOut, byte[] password) {
            this.timeOut = timeOut;
            this.password = password;
        }
    
        public TimeValue timeOut() {
            return timeOut;
        }
    
        /**
         * Returns a copy
         */
        public byte[] password() {
            return password.clone();
        }
    
        @Override
        public String toString() {
            String passwordStr;
            if (password == null) {
                passwordStr = "null";
            } else if (password.length == 0) {
                passwordStr = "";
            } else {
                // just output a hash
                passwordStr = String.format("0x%08x", password.hashCode());
                // BigInteger bi = new BigInteger(1, password);
                // passwordStr = String.format("0x%0" + (password.length << 1) +
                // "X", bi);
            }
            return MoreObjects.toStringHelper(this).add("password", passwordStr)
                    .add("timeOut", timeOut)
                    .toString();
        }
    
        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Parameters other = (Parameters) obj;
            return Arrays.equals(password, other.password)
                    && Objects.equal(timeOut, other.timeOut);
        }
    
        @Override
        public int hashCode() {
            return Objects.hashCode(password);
        }
    }
}
