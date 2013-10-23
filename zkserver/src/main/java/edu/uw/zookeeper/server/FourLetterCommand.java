package edu.uw.zookeeper.server;

import java.lang.annotation.*;

import edu.uw.zookeeper.protocol.FourLetterWord;

@Inherited
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface FourLetterCommand {
    FourLetterWord value();
}
