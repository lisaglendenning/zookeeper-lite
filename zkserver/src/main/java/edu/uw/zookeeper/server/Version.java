package edu.uw.zookeeper.server;

import java.util.Properties;

public abstract class Version {

    public static edu.uw.zookeeper.Version getDefault() {
        return edu.uw.zookeeper.Version.fromPom(getPom());
    }

    public static String getArtifact() {
        return "zkserver";
    }
    
    public static String getProjectName() {
        return "ZooKeeper-Lite Server";
    }
    
    public static Properties getPom() {
        return edu.uw.zookeeper.Version.getPom(
                edu.uw.zookeeper.Version.getGroup(), getArtifact());
    }
    
    private Version() {}
}
