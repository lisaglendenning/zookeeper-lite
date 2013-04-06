package org.apache.zookeeper.util;

import static com.google.common.base.Preconditions.*;

import java.util.concurrent.atomic.AtomicReference;

public interface AutomataState<T extends AutomataState<T>> {
	
	public static class Reference<T extends AutomataState<T>> implements AtomicUpdater<T> {
		
		public static <T extends AutomataState<T>> Reference<T> create(T state) {
			return new Reference<T>(state);
		}
		
		protected final AtomicReference<T> state;
		
		protected Reference(T state) {
			this.state = new AtomicReference<T>(checkNotNull(state));
		}
		
		@Override
		public T get() {
			return state.get();
		}

		@Override
	    public boolean set(T nextState) {
	        return (getAndSet(nextState) != null);
	    }

		@Override
	    public T getAndSet(T nextState) {
	        T prevState = get();
	        if (! compareAndSet(prevState, nextState)) {
	            return null;
	        }
	        return prevState;
	    }

		@Override
	    public boolean compareAndSet(T prevState, T nextState) {
	        if (! prevState.validTransition(nextState)) {
	            return false;
	        }
	        return state.compareAndSet(prevState, nextState);
	    }
	}
	
    boolean isTerminal();
    boolean validTransition(T nextState);
}
