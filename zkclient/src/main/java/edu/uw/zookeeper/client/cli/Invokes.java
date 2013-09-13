package edu.uw.zookeeper.client.cli;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Invokes {
    Class<?>[] commands();
}
