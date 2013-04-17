package edu.uw.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;


import com.google.common.base.Objects;

import edu.uw.zookeeper.util.AutomataState;
import edu.uw.zookeeper.util.TimeValue;

public class Session {

    public static final long UNINITIALIZED_ID = 0;
    public static final Session UNINITIALIZED = new Session();

    public static enum State implements AutomataState<State> {
        SESSION_UNINITIALIZED {
        },
        SESSION_OPENED {
        },
        SESSION_EXPIRED {
        },
        SESSION_CLOSED {
        };

        @Override
        public boolean isTerminal() {
            switch (this) {
            case SESSION_CLOSED:
                return true;
            default:
                return false;
            }
        }

        @Override
        public boolean validTransition(State nextState) {
            checkNotNull(nextState);
            boolean valid = false;
            if (this == nextState) {
                valid = true;
            } else {
                switch (this) {
                case SESSION_UNINITIALIZED:
                    valid = (nextState == SESSION_OPENED);
                    break;
                case SESSION_OPENED:
                    valid = (nextState != SESSION_UNINITIALIZED);
                    break;
                case SESSION_EXPIRED:
                    valid = (nextState == SESSION_CLOSED);
                    break;
                case SESSION_CLOSED:
                    valid = false;
                    break;
                default:
                    break;
                }
            }
            return valid;
        }
    }
    

    public static class Parameters {
    
        public static final long NEVER_TIMEOUT = 0;
        public static final byte[] NO_PASSWORD = new byte[0];
        public static final int PASSWORD_LENGTH = 16;
        public static final TimeUnit TIMEOUT_UNIT = TimeUnit.MILLISECONDS;
    
        private final TimeValue timeOut;
        private final byte[] password;
    
        public static Parameters create() {
            return new Parameters();
        }
    
        public static Parameters create(long timeOut) {
            return new Parameters(timeOut);
        }
    
        public static Parameters create(long timeOut, byte[] password) {
            return new Parameters(timeOut, password);
        }
    
        public static Parameters create(TimeValue timeOut, byte[] password) {
            return new Parameters(timeOut, password);
        }
    
        protected Parameters() {
            this(NEVER_TIMEOUT);
        }
    
        protected Parameters(long timeOut) {
            this(timeOut, NO_PASSWORD);
        }
    
        protected Parameters(long timeOut, byte[] password) {
            this(TimeValue.create(timeOut, TIMEOUT_UNIT), password);
        }
    
        protected Parameters(TimeValue timeOut, byte[] password) {
            this.timeOut = timeOut;
            this.password = password;
        }
    
        public TimeValue timeOut() {
            return timeOut;
        }
    
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
                passwordStr = String.format("0x%08X", password.hashCode());
                // BigInteger bi = new BigInteger(1, password);
                // passwordStr = String.format("0x%0" + (password.length << 1) +
                // "X", bi);
            }
            return Objects.toStringHelper(this).add("password", passwordStr)
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
            return Arrays.equals(password(), other.password())
                    && Objects.equal(timeOut(), other.timeOut());
        }
    
        @Override
        public int hashCode() {
            return Objects.hashCode(password, timeOut);
        }
    }


    protected final long id;
    protected final Parameters parameters;

    public static Session create() {
        return UNINITIALIZED;
    }

    public static Session create(long id, Parameters parameters) {
        return new Session(id, parameters);
    }

    protected Session() {
        this(UNINITIALIZED_ID, Parameters.create());
    }

    protected Session(long id, Parameters parameters) {
        this.id = id;
        this.parameters = parameters;
    }

    public long id() {
        return id;
    }

    public boolean initialized() {
        return id() != UNINITIALIZED_ID;
    }

    public Parameters parameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", id())
                .add("parameters", parameters()).toString();
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
        return Objects.equal(id(), other.id())
                && Objects.equal(parameters(), other.parameters());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id(), parameters());
    }
}
