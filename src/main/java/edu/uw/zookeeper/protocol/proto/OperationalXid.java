package edu.uw.zookeeper.protocol.proto;

import java.lang.annotation.*;


@Inherited
@Documented
public @interface OperationalXid {
    OpCodeXid xid();
}
