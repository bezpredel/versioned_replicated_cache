package com.bezpredel.versioned.datastore.actual;


import com.bezpredel.versioned.datastore.Keyed;

import java.util.Map;

public abstract class Entry<T extends Keyed> implements Map.Entry<Object, T>{
    private final int version;
    private Entry<T> next;

    private Entry(int version) {
        this.version = version;
    }

    public Entry<T> getNext() {
        return next;
    }

    public void setNext(Entry<T> next) {
        this.next = next;
    }

    public int getVersion() {
        return version;
    }

    public abstract T getValue();

    public abstract Object getKey();

    public abstract boolean isADeleteRecord();

    public T setValue(T value) {
        throw new UnsupportedOperationException();
    }

    public static <T extends Keyed> Entry<T> createExistingEntry(int version, T value) {
        return new ExistingEntry<T>(version, value);
    }

    public static <T extends Keyed> Entry<T> createRemovedEntry(int version, Object key) {
        return new RemovedEntry<T>(version, key);
    }

    private final static class ExistingEntry<T extends Keyed> extends Entry<T> {
        private final T value;

        private ExistingEntry(int version, T value) {
            super(version);
            this.value = value;
        }

        @Override
        public T getValue() {
            return value;
        }

        @Override
        public Object getKey() {
            return value.getKey();
        }

        @Override
        public boolean isADeleteRecord() {
            return false;
        }

        /*
        * To fulfill Map.Entry contract
        */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ExistingEntry that = (ExistingEntry) o;

            if (value != null ? !value.equals(that.value) : that.value != null) return false;

            return true;
        }

        /*
         * To fulfill Map.Entry contract
         */
        @Override
        public int hashCode() {
            return value != null ? value.hashCode() : 0;
        }
    }

    private final static class RemovedEntry<T extends Keyed> extends Entry<T> {
        private final Object key;

        private RemovedEntry(int version, Object key) {
            super(version);
            this.key = key;
        }

        @Override
        public T getValue() {
            return null;
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public boolean isADeleteRecord() {
            return true;
        }

        /*
        * To fulfill Map.Entry contract
        */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            RemovedEntry that = (RemovedEntry) o;

            if (key != null ? !key.equals(that.key) : that.key != null) return false;

            return true;
        }

        /*
         * To fulfill Map.Entry contract
         */
        @Override
        public int hashCode() {
            return key != null ? key.hashCode() : 0;
        }
    }
}
