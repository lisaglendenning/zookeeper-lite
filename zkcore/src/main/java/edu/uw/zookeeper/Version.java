package edu.uw.zookeeper;

import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.CaseFormat;
import com.google.common.base.Objects;

public class Version {
    
    public static enum PomProperty {
        VERSION, GROUP_ID, ARTIFACT_ID;
        
        @Override
        public String toString() {
            return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, name());
        }
    }
    
    public static Version getDefault() {
        return fromPom(getPom());
    }

    public static Version fromPom(Properties pomProperties) {
        String version = pomProperties.getProperty(PomProperty.VERSION.toString());
        if (version == null) {
            return unknown();
        } else {
            try {
                return fromString(version);
            } catch (IllegalStateException e) {
                return unknown();
            }
        }
    }
    
    public static Version unknown() {
        return new Version(0, 0, 0, "UNKNOWN");
    }

    public static Version fromString(String version) {
            Matcher m = Pattern.compile("(\\d+).(\\d+).(\\d+)(-(\\w+))?").matcher(version);
            int major = Integer.parseInt(m.group(1));
            int minor = Integer.parseInt(m.group(2));
            int patch = Integer.parseInt(m.group(3));
            String label = (m.group(5) == null) ? "" : m.group(5);
            return of(major, minor, patch, label);
    }
    
    public static Version of(int major, int minor, int patch, String label) {
        return new Version(major, minor, patch, label);
    }
    
    public static String getGroup() {
        return "edu.uw.zookeeper.lite";
    }
    
    public static String getArtifact() {
        return "zkcore";
    }
    
    public static Properties getPom() {
        return getPom(getGroup(), getArtifact());
    }

    public static String getProjectName() {
        return "ZooKeeper-Lite Core";
    }
    
    public static Properties getPom(String group, String artifact) {
       Properties pomProperties = new Properties();
       try {
           pomProperties.load(Version.class.getClassLoader().getResourceAsStream(
                   String.format("META-INF/maven/%s/%s/pom.properties", group, artifact)));
       } catch (Exception e) {
       }
       for (PomProperty k: PomProperty.values()) {
           String v = System.getProperty(k.toString());
           if (v != null) {
               pomProperties.setProperty(k.toString(), v);
           }
       }
       return pomProperties;
    }
    
    private final int major;
    private final int minor;
    private final int patch;
    private final String label;

    public Version(int major, int minor, int patch, String label) {
        this.major = major;
        this.minor = minor;
        this.patch = patch;
        this.label = label;
    }

    public int getMajor() {
        return major;
    }

    public int getMinor() {
        return minor;
    }

    public int getPatch() {
        return patch;
    }

    public String getLabel() {
        return label;
    }
    
    @Override
    public String toString() {
        StringBuilder value = new StringBuilder().append(String.format("%d.%d.%d", major, minor, patch));
        if ((label != null) && ! label.isEmpty()) {
            value.append('-').append(label);
        }
        return value.toString();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (! (obj instanceof Version)) {
            return false;
        }
        Version other = (Version) obj;
        return Objects.equal(major, other.major) 
                && Objects.equal(minor, other.minor)
                && Objects.equal(patch, other.patch)
                && Objects.equal(label, other.label);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(major, minor, patch, label);
    }
}
