package edu.uw.zookeeper.protocol.proto;

import java.lang.annotation.*;


@Inherited
@Documented
public @interface Operational {
    OpCode opcode();
}
