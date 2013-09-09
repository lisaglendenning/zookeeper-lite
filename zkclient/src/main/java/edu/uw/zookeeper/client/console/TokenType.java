package edu.uw.zookeeper.client.console;

public enum TokenType {
    BOOLEAN, STRING, PATH, INTEGER, MODE;

    public String getUsage() {
        switch (this) {
        case BOOLEAN:
            return BooleanArgument.getUsage();
        case STRING:
            return "\"...\"";
        case PATH:
            return "/...";
        case INTEGER:
            return "int";
        case MODE:
            return ModeArgument.getUsage();
        }
        return null;
    }
}
