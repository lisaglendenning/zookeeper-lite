package edu.uw.zookeeper.client.console;

import jline.console.completer.Completer;
import jline.console.completer.EnumCompleter;

import com.google.common.base.Joiner;

import edu.uw.zookeeper.data.CreateMode;

public enum ModeArgument {
    P, PS, E, ES;
    
    protected static String USAGE = Joiner.on(',')
            .appendTo(new StringBuilder().append('{'), values()).append('}').toString();

    public static Completer getCompleter() {
        return new EnumCompleter(ModeArgument.class);
    }

    public static String getUsage() {
        return USAGE;
    }

    public static ModeArgument fromString(String value) {
        for (ModeArgument e : values()) {
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
