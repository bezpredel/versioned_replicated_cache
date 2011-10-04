package com.bezpredel.collections;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public abstract class SerializablePseudoEnum extends PseudoEnum implements Serializable {
    private static final long serialVersionUID = -2760785997543998367L;
    private final SerializationProxy serializationProxy;

    protected SerializablePseudoEnum(Object instanceIdentity) {
        serializationProxy = INSTANCE_DICTIONARY.put(getClassIdentity(), instanceIdentity, this);
    }

    protected Object getClassIdentity() {
        return this.getClass();
    }

    public Object getInstanceIdentity() {
        return serializationProxy.instanceIdentity;
    }

    protected Object writeReplace() throws ObjectStreamException {
        return serializationProxy;
    }

    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        throw new InvalidObjectException("Proxy required");
    }

    private static class SerializationProxy implements Serializable {
        private static final long serialVersionUID = -3770130767921679080L;
        private final Object classIdentity;
        private final Object instanceIdentity;

        private SerializationProxy(Object classIdentity, Object instanceIdentity) {
            this.classIdentity = classIdentity;
            this.instanceIdentity = instanceIdentity;
        }

        Object readResolve() {
            return INSTANCE_DICTIONARY.get(classIdentity, instanceIdentity);
        }
    }

    private static final InstanceDictionary INSTANCE_DICTIONARY = new InstanceDictionary();

    private static class InstanceDictionary {
        private final Map<Object, Class<? extends SerializablePseudoEnum>> classIdentityMap = new HashMap<Object, Class<? extends SerializablePseudoEnum>>();

        private final Map<Object, Map<Object, SerializablePseudoEnum>> instanceMap = new HashMap<Object, Map<Object, SerializablePseudoEnum>>();

        SerializablePseudoEnum get(Object classIdentity, Object instanceIdentity) {
            Map<Object, SerializablePseudoEnum> map = instanceMap.get(classIdentity);
            return map == null ? null : map.get(instanceIdentity);
        }

        void validateClassIdentity(Object classIdentity, SerializablePseudoEnum obj) {
            Class<? extends SerializablePseudoEnum> oldClass = classIdentityMap.get(classIdentity);
            if (oldClass != null) {
                if (oldClass != obj.getClass()) {
                    throw new IllegalStateException("Class identity " + classIdentity + " is already mapped to " + oldClass + ", cannot remap it to " + obj.getClass());
                }
            } else {
                classIdentityMap.put(classIdentity, obj.getClass());
            }
        }

        SerializationProxy put(Object classIdentity, Object instanceIdentity, SerializablePseudoEnum obj) {
            validateClassIdentity(classIdentity, obj);

            Map<Object, SerializablePseudoEnum> map = instanceMap.get(classIdentity);
            if (map == null) {
                map = new HashMap<Object, SerializablePseudoEnum>();
                instanceMap.put(classIdentity, map);
            }

            if (map.containsKey(instanceIdentity)) {
                throw new IllegalStateException(instanceIdentity + " is already registered for " + classIdentity);
            }

            map.put(instanceIdentity, obj);
            return new SerializationProxy(classIdentity, instanceIdentity);
        }
    }
}
