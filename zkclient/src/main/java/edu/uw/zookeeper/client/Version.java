package edu.uw.zookeeper.client;

import java.util.Properties;

public abstract class Version {

    public static edu.uw.zookeeper.Version getDefault() {
        return edu.uw.zookeeper.Version.fromPom(getPom());
    }

    public static String getArtifact() {
        return "zkclient";
    }
    
    public static String getProjectName() {
        return "ZooKeeper-Lite Client";
    }
    
    public static Properties getPom() {
        return edu.uw.zookeeper.Version.getPom(
                edu.uw.zookeeper.Version.getGroup(), getArtifact());
    }
    
    private Version() {}
}
