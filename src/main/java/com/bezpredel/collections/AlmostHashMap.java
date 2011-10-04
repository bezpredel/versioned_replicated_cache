package com.bezpredel.collections;

import com.bezpredel.versioned.datastore.Keyed;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Implementation notes:
 * There is no explicit thread-safety in this class, however it holds the following property:
 * If there is an external happens-after relationship between a put operation and a read operation, then
 * the iterator is guaranteed to see at least all the puts that happened up to the said put operation.
 * There is no such strong guarantee for remove operations, however if the user of this interface only
 * removes objects that the reads are guaranteed not to care about, the iterator won't break.
 *
 *
 *
 *
 * @param <T>
 */
public class AlmostHashMap<T extends Keyed> {
    private ExpandingState<T> state;

    private double removeToSizeRationShrinkTrigger = 0.5;


    public AlmostHashMap(int initialCapacity, double loadFactor, double removeToSizeRationShrinkTrigger) {
        this.removeToSizeRationShrinkTrigger = removeToSizeRationShrinkTrigger;
        int[] table = new int[determineTableSize(initialCapacity, loadFactor)];
        T[] values = (T[]) new Keyed[determineValuesSize(initialCapacity)];
        int[] next = new int[values.length];

        state = new ExpandingState(table, next, values, 0);
    }


    protected final int tableIndexFor(Object key, int tableLength) {
        return (hash(key!=null ? key.hashCode() : 0)) & (tableLength - 1);
    }

    public boolean contains(T object) {
        return get(object.getKey()) != null;
    }

    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    public T get(Object key) {
        ExpandingState<T> state = this.state;

        int len = state.values.length + 1 /* +1 accounts for the 0 mapping to NULL and 1 mapping to 0 index*/;

        int tix = tableIndexFor(key, state.table.length);

        int ix = state.table[tix];

        while (ix != 0 && ix < len) {
            T val = state.values[ix - 1];
            if (val != null && areEqual(key, val.getKey())) {
                return val;
            } else {
                ix = state.next[ix - 1];
            }
        }

        return null;
    }

    protected final boolean areEqual(Object k1, Object k2) {
        return (k1==k2) || (k1!=null && k1.equals(k2));
    }


    // todo: in order to perform well with frequent removes/inserts, especially of the same key, we can replace null (previously-removed) values for the matching hashcode. This should not in theory weaken the guarantees of this class.
    public boolean put(T value) {
        // assert write exclusivity
        ExpandingState<T> state = this.state;
        if(state.size == state.values.length) {
            state = growCapacity(state);
        }
        Object key = value.getKey();
        int tix = tableIndexFor(key, state.table.length);

        int ix = state.table[tix];
        int valueIndex = state.size;

        if(ix == 0) {
            state.table[tix] = valueIndex + 1;
        } else {
            while (true) {
                T val = state.values[ix - 1];
                if (val != null && areEqual(key, val.getKey())) {
                    return false;
                } else {
                    if(state.next[ix - 1]==0) {
                        state.next[ix - 1] = valueIndex + 1;
                        break;
                    } else {
                        ix = state.next[ix - 1];
                    }
                }
            }
        }

        state.values[valueIndex] = value;
        state.size++;

        return true;
    }

    public boolean removeObject(T object) {
        return removeKey(object.getKey());
    }

    public boolean removeKey(Object key) {
        ExpandingState<T> state = this.state;
        int tix = tableIndexFor(key, state.table.length);

        int ix = state.table[tix];

        while (ix != 0) {
            T val = state.values[ix - 1];
            if (val != null && areEqual(key, val.getKey())) {
                state.values[ix - 1] = null;

                state.dirtyRemoveCount++;

//                System.out.println(dirtyRemoveCount);
                if(state.dirtyRemoveCount >= removeToSizeRationShrinkTrigger * state.size ) {
                    shrinkCapacity(state);
                }
                return true;
            } else {
                ix = state.next[ix - 1];
            }
        }

        return false;
    }

    protected ExpandingState<T> growCapacity(ExpandingState<T> state) {
        int[] newTable = new int[state.table.length * 2];
        int[] newNext = new int[state.next.length * 2];
        T[] newValues = (T[])new Keyed[state.values.length * 2];


        // rehash
        System.arraycopy(state.values, 0, newValues, 0, state.size);

        int size = state.size;

        return updateCapacity(newTable, newNext, newValues, size);
    }

    protected ExpandingState<T> shrinkCapacity(ExpandingState<T> state) {
        int cnt = 0;
        final int s = state.size;

        for (int i = 0; i < s; i++) {
            if (state.values[i] != null) {
                cnt++;
            }
        }

        int newCapacity = determineValuesSize(cnt) * 2;

        if(shouldShrinkCapacity(state, newCapacity)) {
//            System.out.println("Shrinking from " + values.length + " to " + newCapacity);
            int multiplier = state.values.length / newCapacity; // both numbers are powers of 2, so the result is also a power of 2

            T[] newValues = (T[])new Keyed[newCapacity];
            int[] newNext = new int[newCapacity];
            int[] newTable = new int[state.table.length / multiplier];
            int newSize = 0;
            for (int i = 0; i < s; i++) {
                if (state.values[i] != null) {
                    newValues[newSize++] = state.values[i];
                }
            }

//            System.out.println(size + " -> " + newSize + "(" + values.length + " -> " + newValues.length+")");
            return updateCapacity(newTable, newNext, newValues, newSize);

        } else {
            return state;
        }
    }

    protected ExpandingState<T> updateCapacity(final int[] newTable, final int[] newNext, final T[] newValues /*populated*/, final int size) {
        for(int i=0; i < size; i++) {
            T value = newValues[i];
            if (value != null) {
                int tix = tableIndexFor(value.getKey(), newTable.length);
                int ix = newTable[tix];

                if(ix == 0) {
                    newTable[tix] = i + 1;
                } else {
                    assert newNext[ix - 1] != 0;

                    while(newNext[ix - 1] != 0) {
                        ix = newNext[ix - 1];
                    }

                    newNext[ix - 1] = i + 1;
                }
            }
        }

        ExpandingState<T> state = new ExpandingState<T>(newTable, newNext, newValues, size);

        this.state = state;

        return state;
    }


    /**
     * To be called only from a write lock
     */
    public boolean isEmpty() {
        ExpandingState<T> state = this.state;
        return state.size == state.dirtyRemoveCount;
    }

    /**
     * Will NOT give accurate information, for diagnostic purposes only
     */
    public int count() {
        T[] values = this.state.values;
        int cnt = 0;
        for(int i=0; i<values.length; i++) {
            if(values[i]!=null) {
                cnt++;
            }
        }
        return cnt;
    }

    private boolean shouldShrinkCapacity(ExpandingState<T> state, int newCapacity) {
        assert newCapacity <= state.values.length;

        return (newCapacity < state.values.length);
    }


    private int determineTableSize(int initialCapacity, double loadFactor) {
        int capacity = 1;
        initialCapacity = (int) (initialCapacity / loadFactor);
        while (capacity < initialCapacity)
            capacity <<= 1;
        return capacity;
    }

    private int determineValuesSize(int initialCapacity) {
        int capacity = 1;
        while (capacity < initialCapacity)
            capacity <<= 1;
        return capacity;
    }


    static int hash(int h) {
        h ^= (h >>> 20) ^ (h >>> 12);
        return h ^ (h >>> 7) ^ (h >>> 4);
    }

    public Iterator<T> valueIterator() {
        return new Iter(state);
    }

    // skips nulls, does not throw concurrent mod
    private class Iter implements Iterator<T> {
        private final T[] table;
        private final int size;
        private int pos = 0;
        private T nextValue;

        private Iter(ExpandingState<T> state) {
            // while "size" may be larger than we are interested in (because of lack of sync), two things will always hold true:
            // (1) size < table.length
            // (2) the "extra" items will have wrong versions
            table = state.values;
            size = state.size;
        }

        private void findAnotherValue() {
            while(pos < size && nextValue == null) {
                nextValue = table[pos++];
            }
        }

        public boolean hasNext() {
            findAnotherValue();
            return nextValue!=null;
        }

        public T next() {
            findAnotherValue();
            T retVal = nextValue;
            if(retVal == null) {
                throw new NoSuchElementException();
            } else {
                nextValue = null;
                return retVal;
            }
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private static class ExpandingState<T> {
        private final int[] table;
        private final int[] next;
        private final T[] values;

        private int size;
        private int dirtyRemoveCount;

        private ExpandingState(int[] table, int[] next, T[] values, int size) {
            this.table = table;
            this.next = next;
            this.values = values;
            this.size = size;
        }
    }

}
