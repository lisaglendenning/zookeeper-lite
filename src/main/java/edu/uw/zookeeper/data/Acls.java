package edu.uw.zookeeper.data;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;

import com.google.common.base.Objects;
import com.google.common.collect.ForwardingSet;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import edu.uw.zookeeper.protocol.Operation;
import edu.uw.zookeeper.protocol.Records;
import edu.uw.zookeeper.util.AbstractPair;
import edu.uw.zookeeper.util.Reference;

public abstract class Acls {
    
    public static enum Permission {
        NONE(0),
        CREATE(ZooDefs.Perms.CREATE),
        READ(ZooDefs.Perms.READ),
        WRITE(ZooDefs.Perms.WRITE),
        DELETE(ZooDefs.Perms.DELETE),
        ADMIN(ZooDefs.Perms.ADMIN);
        
        public static Permission valueOf(int value) {
            for (Permission p: Permission.values()) {
                if (value == p.intValue()) {
                    return p;
                }
            }
            return null;
        }
        
        private final int value;
        
        private Permission(int value) {
            this.value = value;
        }
        
        public int intValue() {
            return value;
        }
    }
    
    public static class PermissionSet extends ForwardingSet<Permission> {

        public static PermissionSet of(Permission permission) {
            return of(EnumSet.of(permission));
        }

        public static PermissionSet of(Set<Permission> permissions) {
            return new PermissionSet(permissions);
        }
        
        public static ImmutablePermissionSet immutableOf(Permission permission) {
            return ImmutablePermissionSet.of(permission);
        }

        public static ImmutablePermissionSet immutableOf(ImmutableSet<Permission> permissions) {
            return ImmutablePermissionSet.of(permissions);
        }
        
        public static final ImmutablePermissionSet ALL = immutableOf(Sets.immutableEnumSet(EnumSet.allOf(Permission.class)));
        
        protected final Set<Permission> delegate;
        
        protected PermissionSet(Set<Permission> delegate) {
            this.delegate = delegate;
        }
        
        @Override
        protected Set<Permission> delegate() {
            return delegate;
        }
        
        public int intValue() {
            int total = 0;
            for (Permission p: this) {
                total |= p.intValue();
            }
            return total;
        }
        
        public static class ImmutablePermissionSet extends PermissionSet {

            public static ImmutablePermissionSet of(Permission permission) {
                return of(ImmutableSet.of(permission));
            }

            public static ImmutablePermissionSet of(ImmutableSet<Permission> permissions) {
                return new ImmutablePermissionSet(permissions);
            }
            
            protected final int value;
            
            protected ImmutablePermissionSet(ImmutableSet<Permission> delegate) {
                super(delegate);
                this.value = super.intValue();
            }

            @Override
            public int intValue() {
                return value;
            }
        }
    }
    
    public static enum Scheme {
        NONE, WORLD, AUTH, DIGEST, IP;
        
        public static final Id ID_NONE = NONE.idOf("");
        public static final Id ID_ANYONE = WORLD.idOf("anyone");
        public static final Id ID_AUTH = AUTH.idOf("");
        
        public static Scheme of(String value) {
            for (Scheme e: Scheme.values()) {
                if (value.equalsIgnoreCase(e.name())) {
                    return e;
                }
            }
            return null;
        }
        
        @Override
        public String toString() {
            return name().toLowerCase();
        }
        
        public Id idOf(String id) {
            return new Id(toString(), id);
        }
    }
    
    public static class Acl extends AbstractPair<PermissionSet, Id> implements Operation.RecordHolder<ACL> {

        public static Acl of(PermissionSet permissions, Id id) {
            return new Acl(permissions, id);
        }
        
        public static List<ACL> asRecordList(List<Acl> acls) {
            List<ACL> records = new ArrayList<ACL>(acls.size());
            for (Acl acl: acls) {
                records.add(acl.asRecord());
            }
            return records;
        }
        
        protected Acl(PermissionSet permissions, Id id) {
            super(permissions, id);
        }
        
        public PermissionSet permissions() {
            return first;
        }
        
        public Id id() {
            return second;
        }
        
        @Override
        public ACL asRecord() {
            return new ACL(permissions().intValue(), id());
        }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this)
                    .add("id", Records.toString(id())).add("permissions", permissions()).toString();
        }
    }
    
    public static enum Definition implements Reference<Acl> {
        NONE(Acl.of(PermissionSet.immutableOf(Permission.NONE), Scheme.ID_NONE)),
        ANYONE_ALL(Acl.of(PermissionSet.ALL, Scheme.ID_ANYONE)),
        ANYONE_READ(Acl.of(PermissionSet.immutableOf(Permission.READ), Scheme.ID_ANYONE)),
        AUTH_ALL(Acl.of(PermissionSet.ALL, Scheme.ID_AUTH));
        
        private Acl acl;
        private List<Acl> asList;
        private List<ACL> asRecord;
        
        private Definition(Acl acl) {
            this.acl = acl;
            this.asList = ImmutableList.of(acl);
            this.asRecord = ImmutableList.of(acl.asRecord());
        }
        
        @Override
        public Acl get() {
            return acl;
        }
        
        public List<Acl> asList() {
            return asList;
        }
        
        public List<ACL> asRecordList() {
            return asRecord;
        }
    }
    
    private Acls() {}
}
