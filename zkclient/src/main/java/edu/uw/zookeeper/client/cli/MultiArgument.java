package edu.uw.zookeeper.client.cli;

public enum MultiArgument {
    BEGIN, CANCEL, END;

    public static MultiArgument fromString(String value) {
        for (MultiArgument e : values()) {
            if (e.toString().equals(value)) {
                return e;
            }
        }
        return null;
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}