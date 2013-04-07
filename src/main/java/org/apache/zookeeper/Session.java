package org.apache.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.zookeeper.data.OpCreateSessionAction;
import org.apache.zookeeper.util.AutomataState;

import com.google.common.base.Objects;

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

    protected final long id;
    protected final SessionParameters parameters;

    public static Session create() {
        return UNINITIALIZED;
    }

    public static Session create(OpCreateSessionAction.Response message) {
        return new Session(message.record().getSessionId(),
                SessionParameters.create(message));
    }

    public static Session create(long id, SessionParameters parameters) {
        return new Session(id, parameters);
    }

    public Session() {
        this(UNINITIALIZED_ID, new SessionParameters());
    }

    public Session(long id, SessionParameters parameters) {
        this.id = id;
        this.parameters = parameters;
    }

    public long id() {
        return id;
    }

    public boolean initialized() {
        return id() != UNINITIALIZED_ID;
    }

    public SessionParameters parameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this).add("id", id())
                .add("parameters", parameters()).toString();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Session other = (Session) obj;
        return Objects.equal(id(), other.id())
                && Objects.equal(parameters(), other.parameters());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id(), parameters());
    }
}
