package org.apache.zookeeper;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.zookeeper.protocol.OpCreateSessionAction;
import org.apache.zookeeper.util.AutomataState;

import com.google.common.base.Objects;

public class Session {
    
    public static final long UNINITIALIZED_ID = 0;

    public static enum State implements AutomataState<State> {
        OPENING {},
        OPENED {}, 
        CLOSED {
            @Override
            public boolean isTerminal() {
                return true;
            }
        }, 
        EXPIRED {
            @Override
            public boolean isTerminal() {
                return true;
            }
        };
        
        @Override
        public boolean isTerminal() {
            return false;
        }

        @Override
        public State initial() {
            return OPENING;
        }
        
        @Override
        public boolean validTransition(State nextState) {
            checkNotNull(nextState);
            boolean valid = false;
            if (this == nextState) {
                valid = true;
            } else {
                switch (this) {
                case OPENING:
                    valid = true;
                    break;
                case OPENED:
                    valid = (nextState != OPENING);
                    break;
                case CLOSED:
                    valid = (nextState == EXPIRED);
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
        return new Session();
    }

    public static Session create(OpCreateSessionAction.Response message) {
        return new Session(message.response().getSessionId(),
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

    public SessionParameters parameters() {
        return parameters;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("id", id())
                .add("parameters", parameters())
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
        Session other = (Session) obj;
        return Objects.equal(id(), other.id()) 
                && Objects.equal(parameters(), other.parameters());
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(id(), parameters());
    }
}
