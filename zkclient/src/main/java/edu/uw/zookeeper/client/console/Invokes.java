package edu.uw.zookeeper.client.console;

import java.lang.annotation.*;

@Documented
@Inherited
@Retention(RetentionPolicy.RUNTIME)
public @interface Invokes {
    ConsoleCommand[] commands();
}
