package edu.uw.zookeeper;

import com.google.common.base.Function;
import com.google.common.base.Optional;

public enum QuorumRole implements Function<QuorumRole, Optional<QuorumRole>> {
    UNKNOWN {
        @Override
        public Optional<QuorumRole> apply(QuorumRole input) {
            switch (input) {
            case UNKNOWN:
                return Optional.absent();
            default:
                return Optional.of(input);
            }
        }},
    LOOKING {
        @Override
        public Optional<QuorumRole> apply(QuorumRole input) {
            switch (input) {
            case LOOKING:
                return Optional.absent();
            case LEADING:
            case FOLLOWING:
                return Optional.of(input);
            default:
                throw new IllegalStateException();
            }
        }},
    FOLLOWING {
        @Override
        public Optional<QuorumRole> apply(QuorumRole input) {
            switch (input) {
            case FOLLOWING:
                return Optional.absent();
            case LOOKING:
            case LEADING:
                return Optional.of(input);
            default:
                throw new IllegalStateException();
            }
        }},
    LEADING {
        @Override
        public Optional<QuorumRole> apply(QuorumRole input) {
            switch (input) {
            case LEADING:
                return Optional.absent();
            case LOOKING:
            case FOLLOWING:
                return Optional.of(input);
            default:
                throw new IllegalStateException();
            }
        }},
    OBSERVING {
        @Override
        public Optional<QuorumRole> apply(QuorumRole input) {
            switch (input) {
            case OBSERVING:
                return Optional.absent();
            default:
                throw new IllegalStateException();
            }
        }};
}