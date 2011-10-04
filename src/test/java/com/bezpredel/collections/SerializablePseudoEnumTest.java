package com.bezpredel.collections;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;

import static org.junit.Assert.*;

public class SerializablePseudoEnumTest {
    @Test(expected = IllegalStateException.class)
    public void testDuplicateInstanceIdentity() throws Exception {
        SPE2 e1 = new SPE2("duplicate");
        SPE2 e2 = new SPE2("duplicate");
    }


    @Test
    public void testSerialization() throws Exception {
        SPE e1 = new SPE("a");
        SPE e2 = new SPE("b");
        SPE e3 = new SPE("c");

        assertEquals(3, PseudoEnum.getCurrentCapacity(SPE.class));

        assertSame(e1, deserializeObject(serializeObject(e1)));
        assertEquals(3, PseudoEnum.getCurrentCapacity(SPE.class));
        assertSame(e2, deserializeObject(serializeObject(e2)));
        assertEquals(3, PseudoEnum.getCurrentCapacity(SPE.class));

        Object proxyForExistent = makeProxyInstance(SPE.class, "a");

        assertSame(e1, deserializeObject(serializeObject(proxyForExistent)));

        Object proxyForNonExistent = makeProxyInstance(SPE.class, "d");

        assertNull(deserializeObject(serializeObject(proxyForNonExistent)));

    }

    private Object makeProxyInstance(Object classId, Object instanceId) throws Exception {

        Class clazz = Class.forName("com.bezpredel.collections.SerializablePseudoEnum$SerializationProxy");
        Constructor constructor = clazz.getDeclaredConstructor(Object.class, Object.class);
        constructor.setAccessible(true);

        return constructor.newInstance(classId, instanceId);
    }

    private byte[] serializeObject(Object o) throws Exception{
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(o);
        oos.close();
        return baos.toByteArray();
    }

    private Object deserializeObject(byte[] data) throws Exception {
        ObjectInputStream is = new ObjectInputStream(new ByteArrayInputStream(data));
        try {
            return is.readObject();
        } finally {
            is.close();
        }
    }

    private static class SPE extends SerializablePseudoEnum {
        private static final long serialVersionUID = -4432030786735592318L;

        public SPE(String name) {
            super(name);
        }

    }

    private static class SPE2 extends SerializablePseudoEnum {
        private static final long serialVersionUID = -4432030786735592318L;

        public SPE2(String name) {
            super(name);
        }

    }
}
