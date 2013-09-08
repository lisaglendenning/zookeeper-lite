package edu.uw.zookeeper.client.console;

public enum TokenType {
    BOOLEAN, STRING, PATH;

    public String getUsage() {
        switch (this) {
        case BOOLEAN:
            return BooleanArgument.getUsage();
        case STRING:
            return "\"...\"";
        case PATH:
            return "/...";
        }
        return null;
    }
}
