package com.bezpredel.collections;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public abstract class PseudoEnum {
    private transient final int ordinal;

    protected PseudoEnum() {
        ordinal = getOrdinal(this.getClass(), this);
    }

    protected int getOrdinal() {
        return ordinal;
    }

    private static final Map<Class<? extends PseudoEnum>, Integer> counters = new HashMap<Class<? extends PseudoEnum>, Integer>();
    private static final Map<Class<? extends PseudoEnum>, ArrayList<PseudoEnum>> items = new HashMap<Class<? extends PseudoEnum>, ArrayList<PseudoEnum>>();

    private static int getOrdinal(Class<? extends PseudoEnum> clazz, PseudoEnum item) {
        synchronized (counters) {
            Integer i = counters.get(clazz);
            if(i==null) {
                counters.put(clazz, Integer.valueOf(1));

                ArrayList<PseudoEnum> a = new ArrayList<PseudoEnum>();
                a.add(item);

                items.put(clazz, a);
                return 0;
            } else {
                counters.put(clazz, Integer.valueOf(i.intValue() + 1));

                items.get(clazz).add(item);

                return i;
            }
        }
    }

    public static <E extends PseudoEnum> E get(Class<E> clazz, int index) {
        return (E)items.get(clazz).get(index);
    }

    public static int getCurrentCapacity(Class<? extends PseudoEnum> clazz) {
        try {
            Integer i = counters.get(clazz);
            return i==null ? 0 : i.intValue();
        } catch (Exception e) {
            return 0;
        }
    }

    @Override
    public final boolean equals(Object obj) {
        return (this == obj);
    }

    @Override
    public final int hashCode() {
        return ordinal;
    }

    public static <T extends PseudoEnum> Comparator<T> getByOrdinalComparator() {
        return new Comparator<T>() {
            public int compare(T o1, T o2) {
                return o1.getOrdinal() - o2.getOrdinal();
            }
        };
    }
}
