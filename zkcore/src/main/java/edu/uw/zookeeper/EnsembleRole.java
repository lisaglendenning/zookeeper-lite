package edu.uw.zookeeper;

import com.google.common.base.Function;
import com.google.common.base.Optional;

import edu.uw.zookeeper.common.Automaton;

public enum EnsembleRole implements Function<EnsembleRole, Optional<Automaton.Transition<EnsembleRole>>> {
    UNKNOWN {
        @Override
        public Optional<Automaton.Transition<EnsembleRole>> apply(EnsembleRole input) {
            switch (input) {
            case UNKNOWN:
                return Optional.absent();
            default:
                return Optional.of(Automaton.Transition.<EnsembleRole>create(this, input));
            }
        }},
    LOOKING {
        @Override
        public Optional<Automaton.Transition<EnsembleRole>> apply(EnsembleRole input) {
            switch (input) {
            case LOOKING:
                return Optional.absent();
            case LEADING:
            case FOLLOWING:
                return Optional.of(Automaton.Transition.<EnsembleRole>create(this, input));
            default:
                throw new IllegalStateException();
            }
        }},
    FOLLOWING {
        @Override
        public Optional<Automaton.Transition<EnsembleRole>> apply(EnsembleRole input) {
            switch (input) {
            case FOLLOWING:
                return Optional.absent();
            case LOOKING:
            case LEADING:
                return Optional.of(Automaton.Transition.<EnsembleRole>create(this, input));
            default:
                throw new IllegalStateException();
            }
        }},
    LEADING {
        @Override
        public Optional<Automaton.Transition<EnsembleRole>> apply(EnsembleRole input) {
            switch (input) {
            case LEADING:
                return Optional.absent();
            case LOOKING:
            case FOLLOWING:
                return Optional.of(Automaton.Transition.<EnsembleRole>create(this, input));
            default:
                throw new IllegalStateException();
            }
        }},
    OBSERVING {
        @Override
        public Optional<Automaton.Transition<EnsembleRole>> apply(EnsembleRole input) {
            switch (input) {
            case OBSERVING:
                return Optional.absent();
            default:
                throw new IllegalStateException();
            }
        }};
}