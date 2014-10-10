package edu.uw.zookeeper.data;

import com.google.common.collect.ImmutableSet;

public enum CreateMode {
    PERSISTENT(CreateFlag.PERSISTENT),
    PERSISTENT_SEQUENTIAL(CreateFlag.PERSISTENT, CreateFlag.SEQUENTIAL),
    EPHEMERAL(CreateFlag.EPHEMERAL),
    EPHEMERAL_SEQUENTIAL(CreateFlag.EPHEMERAL, CreateFlag.SEQUENTIAL);

    public static CreateMode valueOf(int value) {
        switch (value) {
        case 0: 
            return PERSISTENT;
        case 1: 
            return EPHEMERAL;
        case 2: 
            return PERSISTENT_SEQUENTIAL;
        case 3: 
            return EPHEMERAL_SEQUENTIAL;
        default:
            throw new IllegalArgumentException(String.valueOf(value));
        }
    }

    private final ImmutableSet<CreateFlag> flags;
    private final int value;

    private CreateMode(CreateFlag...flags) {
        this.flags = ImmutableSet.copyOf(flags);
        int value = 0;
        for (CreateFlag f: flags) {
            value = value | f.intValue();
        }
        this.value = value;
    }
    
    public boolean contains(CreateFlag flag) {
        return flags.contains(flag);
    }
    
    public CreateMode sequential() {
        switch (this) {
        case PERSISTENT:
            return PERSISTENT_SEQUENTIAL;
        case EPHEMERAL:
            return EPHEMERAL_SEQUENTIAL;
        default:
            return this;
        }
    }
    
    public CreateMode ephemeral() {
        switch (this) {
        case PERSISTENT:
            return EPHEMERAL;
        case PERSISTENT_SEQUENTIAL:
            return EPHEMERAL_SEQUENTIAL;
        default:
            return this;
        }
    }
    
    public boolean isEphemeral() {
        return contains(CreateFlag.EPHEMERAL);
    }
    
    public boolean isSequential() {
        return contains(CreateFlag.SEQUENTIAL);
    }
    
    public int intValue() {
        return value;
    }
}
