package edu.uw.zookeeper.client.cli;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface CommandDescriptor {
    String[] names() default {};

    String description() default "";

    ArgumentDescriptor[] arguments() default {};
}
