package edu.uw.zookeeper;

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import edu.uw.zookeeper.Cfg.DynamicServer.Role;

@RunWith(JUnit4.class)
public class TestCfg {

    @Test
    public void testServer() {
        int id = 1;
        Cfg.Server value = Cfg.Server.valueOf(id);
        assertEquals(id, value.getId());
        assertEquals(value, Cfg.Server.valueOf(value.toString()));
    }

    @Test
    public void testStaticServer() {
        String hostname = "125.23.63.23";
        int leaderPort = 1234;
        int electionPort = 1235;
        
        String input = Cfg.StaticServer.toString(hostname, leaderPort, electionPort);
        Cfg.StaticServer value = Cfg.StaticServer.valueOf(input);
        staticServerEquals(value, hostname, leaderPort, electionPort);
        
        input = Cfg.StaticServer.toString(hostname, leaderPort);
        value = Cfg.StaticServer.valueOf(input);
        staticServerEquals(value, hostname, leaderPort, Cfg.StaticServer.DEFAULT_ELECTION_PORT);
        
        input = Cfg.StaticServer.toString(leaderPort, electionPort);
        value = Cfg.StaticServer.valueOf(input);
        staticServerEquals(value, Cfg.StaticServer.DEFAULT_HOSTNAME, leaderPort, electionPort);
        
        input = Cfg.StaticServer.toString(leaderPort);
        value = Cfg.StaticServer.valueOf(input);
        staticServerEquals(value, Cfg.StaticServer.DEFAULT_HOSTNAME, leaderPort, Cfg.StaticServer.DEFAULT_ELECTION_PORT);
    }
    
    protected static void staticServerEquals(Cfg.StaticServer value, 
            String hostname,
            int leaderPort,
            int electionPort) {
        assertEquals(hostname, value.getHostname());
        assertEquals(leaderPort, value.getLeaderPort());
        assertEquals(electionPort, value.getElectionPort());
    }

    @Test
    public void testDynamicServer() {
        // examples from Apache website
        String address = "125.23.63.23";
        int leaderPort = 1234;
        int electionPort = 1235;
        int clientPort = 1236;
        
        String input = Cfg.DynamicServer.toString(address, leaderPort, electionPort, clientPort);
        Cfg.DynamicServer value = Cfg.DynamicServer.valueOf(input);
        dynamicServerEquals(value, address, leaderPort, electionPort, Cfg.DynamicServer.DEFAULT_ROLE, Cfg.DynamicServer.DEFAULT_CLIENT_ADDRESS, clientPort);
        
        for (Role role: Role.values()) {
            input = Cfg.DynamicServer.toString(address, leaderPort, electionPort, role, clientPort);
            value = Cfg.DynamicServer.valueOf(input);
            dynamicServerEquals(value, address, leaderPort, electionPort, role, Cfg.DynamicServer.DEFAULT_CLIENT_ADDRESS, clientPort);
        }
        
        String clientAddress = "125.23.63.24";
        input = Cfg.DynamicServer.toString(address, leaderPort, electionPort, clientAddress, clientPort);
        value = Cfg.DynamicServer.valueOf(input);
        dynamicServerEquals(value, address, leaderPort, electionPort, Cfg.DynamicServer.DEFAULT_ROLE, clientAddress, clientPort);
        
        Role role = Role.PARTICIPANT;
        input = Cfg.DynamicServer.toString(address, leaderPort, electionPort, role, clientAddress, clientPort);
        value = Cfg.DynamicServer.valueOf(input);
        dynamicServerEquals(value, address, leaderPort, electionPort, role, clientAddress, clientPort);
    }
    
    protected static void dynamicServerEquals(Cfg.DynamicServer value, 
            String address,
            int leaderPort,
            int electionPort,
            Role role,
            String clientAddress,
            int clientPort) {
        assertEquals(address, value.getAddress());
        assertEquals(leaderPort, value.getLeaderPort());
        assertEquals(electionPort, value.getElectionPort());
        assertEquals(role, value.getRole());
        assertEquals(clientAddress, value.getClientAddress());
        assertEquals(clientPort, value.getClientPort());
    }
}
