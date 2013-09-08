package edu.uw.zookeeper.client.console;

public @interface ArgumentDescriptor {
    String name() default "";

    TokenType type() default TokenType.STRING;

    String value() default "";
}
