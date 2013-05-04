package edu.uw.zookeeper.client;

import edu.uw.zookeeper.data.ZNodePath;
import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.proto.Records;
import edu.uw.zookeeper.util.Processor;
import edu.uw.zookeeper.util.Reference;

public class ChrootProcessor implements Processor<Operation.Request, Operation.Request>, Reference<ZNodePath> {

    public static ChrootProcessor newInstance(ZNodePath chroot) {
        return new ChrootProcessor(chroot);
    }
    
    private final ZNodePath chroot;
    
    private ChrootProcessor(ZNodePath chroot) {
        this.chroot = chroot;
    }
    
    @Override
    public ZNodePath get() {
        return chroot;
    }
    
    @Override
    public Operation.Request apply(Operation.Request input) {
        // Modify in place
        if (input instanceof Records.PathHolder) {
            Records.PathHolder pathed = (Records.PathHolder)input;
            pathed.setPath(get().toString() + pathed.getPath());
        }
        return input;
    }
}
