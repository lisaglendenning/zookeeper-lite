package edu.uw.zookeeper.jmx;

import java.io.File;
import java.io.IOException;
import java.util.List;

import javax.management.remote.JMXServiceURL;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import com.google.common.base.Throwables;
import com.sun.tools.attach.VirtualMachine;
import com.sun.tools.attach.VirtualMachineDescriptor;

import edu.uw.zookeeper.common.DefaultsFactory;

public enum SunAttachQueryJmx implements DefaultsFactory<String, JMXServiceURL> {
    SUN_ATTACH_QUERY_JMX;
    
    public static SunAttachQueryJmx getInstance() {
        return SUN_ATTACH_QUERY_JMX;
    }

    public static final String CONNECTOR_ADDRESS =
            "com.sun.management.jmxremote.localConnectorAddress";
    public static final String ZOOKEEPER_PACKAGE =
            "org.apache.zookeeper.server";
    
    private final Logger logger = LogManager.getLogger(SunAttachQueryJmx.class);
    
    public JMXServiceURL get(String id) {
        // From http://docs.oracle.com/javase/6/docs/technotes/guides/management/agent.html
        logger.info("Attaching to JVM: {}", id);
        JMXServiceURL url = null;
        VirtualMachine vm = null;
        try {
            // attach to the target application
            vm = VirtualMachine.attach(id);
    
            // get the connector address
            String connectorAddress = vm.getAgentProperties().getProperty(
                    CONNECTOR_ADDRESS);
    
            // no connector address, so we start the JMX agent
            if (connectorAddress == null) {
                String agent = vm.getSystemProperties().getProperty("java.home")
                        + File.separator + "lib" + File.separator
                        + "management-agent.jar";
                logger.info("Loading agent: {}", agent);
                vm.loadAgent(agent);
    
                // agent is started, get the connector address
                connectorAddress = vm.getAgentProperties().getProperty(
                        CONNECTOR_ADDRESS);
            }

            url = new JMXServiceURL(connectorAddress);
            logger.info("{} = {}", CONNECTOR_ADDRESS, url);
        } catch (Exception e) {
            throw Throwables.propagate(e);
        } finally {
            if (vm != null) {
                try {
                    vm.detach();
                } catch (IOException e) {
                }
            }
        }
        return url;
    }

    @Override
    public JMXServiceURL get() {
        // Guess which VM
        List<VirtualMachineDescriptor> vms = VirtualMachine.list();
        for (VirtualMachineDescriptor vm: vms) {
            if (vm.displayName().contains(ZOOKEEPER_PACKAGE)) {
                logger.info("Found {} JVM: {}", ZOOKEEPER_PACKAGE, vm);
                return get(vm.id());
            }
        }
        logger.warn("No {} JVM found: {}", ZOOKEEPER_PACKAGE, vms);
        return null;
    }

    public static void main(String[] args) throws Exception {
        SunAttachQueryJmx query = SunAttachQueryJmx.getInstance();
        JMXServiceURL url;
        if (args.length > 0) {
            url = query.get(args[0]);
        } else {
            url = query.get();
        }
        if (url != null) {
            System.out.println(url);
        } else {
            System.exit(1);
        }
    }
}
