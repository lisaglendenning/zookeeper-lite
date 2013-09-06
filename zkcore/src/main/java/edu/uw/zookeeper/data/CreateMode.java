package edu.uw.zookeeper.data;

import com.google.common.collect.ImmutableSet;

public enum CreateMode {
    PERSISTENT(CreateFlag.PERSISTENT),
    PERSISTENT_SEQUENTIAL(CreateFlag.PERSISTENT, CreateFlag.SEQUENTIAL),
    EPHEMERAL(CreateFlag.EPHEMERAL),
    EPHEMERAL_SEQUENTIAL(CreateFlag.EPHEMERAL, CreateFlag.SEQUENTIAL);

    static public CreateMode valueOf(int value) {
        switch (value) {
        case 0: 
            return PERSISTENT;
        case 1: 
            return EPHEMERAL;
        case 2: 
            return PERSISTENT_SEQUENTIAL;
        case 3: 
            return EPHEMERAL_SEQUENTIAL ;
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
    
    public int intValue() {
        return value;
    }
}