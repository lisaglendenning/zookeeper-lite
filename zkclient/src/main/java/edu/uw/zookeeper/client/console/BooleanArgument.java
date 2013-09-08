package edu.uw.zookeeper.client.console;

import com.google.common.base.Joiner;

import jline.console.completer.Completer;
import jline.console.completer.EnumCompleter;

public enum BooleanArgument {
    N, NO, FALSE, Y, YES, TRUE;

    protected static String USAGE = Joiner.on(',')
            .appendTo(new StringBuilder().append('{'), values()).append('}').toString();

    public static Completer getCompleter() {
        return new EnumCompleter(BooleanArgument.class);
    }

    public static String getUsage() {
        return USAGE;
    }

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