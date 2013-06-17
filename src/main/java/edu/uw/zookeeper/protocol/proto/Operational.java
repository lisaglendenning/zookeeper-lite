package edu.uw.zookeeper.protocol.proto;

import java.lang.annotation.*;


@Inherited
@Documented
@Retention(RetentionPolicy.RUNTIME)
public @interface Operational {
    OpCode opcode();
}
