package edu.uw.zookeeper.client.console;

import com.google.common.base.Joiner;

import jline.console.completer.Completer;
import jline.console.completer.EnumCompleter;

public enum MultiArgument {
    BEGIN, CANCEL, END;

    protected static String USAGE = Joiner.on(',')
            .appendTo(new StringBuilder().append('{'), values()).append('}').toString();

    public static Completer getCompleter() {
        return new EnumCompleter(MultiArgument.class);
    }

    public static String getUsage() {
        return USAGE;
    }

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