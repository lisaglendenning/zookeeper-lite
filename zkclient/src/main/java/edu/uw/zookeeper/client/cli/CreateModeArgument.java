package edu.uw.zookeeper.client.cli;

import edu.uw.zookeeper.data.CreateMode;

public enum CreateModeArgument {
    P, PS, E, ES;
    
    public static CreateModeArgument fromString(String value) {
        for (CreateModeArgument e : values()) {
            if (e.toString().equals(value)) {
                return e;
            }
        }
        return null;
    }

    public CreateMode value() {
        switch (this) {
        case P:
            return CreateMode.PERSISTENT;
        case PS:
            return CreateMode.PERSISTENT_SEQUENTIAL;
        case E:
            return CreateMode.EPHEMERAL;
        case ES:
            return CreateMode.EPHEMERAL_SEQUENTIAL;
        }
        throw new AssertionError();
    }

    @Override
    public String toString() {
        return name().toLowerCase();
    }
}
