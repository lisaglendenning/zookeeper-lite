package edu.uw.zookeeper.data;

public enum CreateFlag {
    PERSISTENT(0), EPHEMERAL(1), SEQUENTIAL(2);

    public static CreateFlag valueOf(int value) {
        switch (value) {
        case 0:
            return PERSISTENT;
        case 1:
            return EPHEMERAL;
        case 2:
            return SEQUENTIAL;
        default:
            throw new IllegalArgumentException(String.valueOf(value));
        }
    }

    private final int value;
    
    private CreateFlag(int value) {
        this.value = value;
    }
    
    public int intValue() {
        return value;
    }
    
    public boolean memberOf(int flags) {
        return (0 != (value & flags));
    }
}
