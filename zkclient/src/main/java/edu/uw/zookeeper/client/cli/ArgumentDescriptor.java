package edu.uw.zookeeper.client.cli;

public @interface ArgumentDescriptor {
    String name() default "";

    String value() default "";
    
    TokenType token() default TokenType.STRING;

    Class<?> type() default Void.class;
}
