package edu.uw.zookeeper.client.cli;

public enum BooleanArgument {
    N, NO, FALSE, Y, YES, TRUE;

    public static BooleanArgument fromString(String value) {
        for (BooleanArgument e : values()) {
            if (e.toString().equals(value)) {
                return e;
            }
        }
        return null;
    }

    public boolean booleanValue() {
        switch (this) {
        case Y:
        case YES:
        case TRUE:
            return true;
        default:
            return false;
        }
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}