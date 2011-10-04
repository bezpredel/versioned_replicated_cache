package com.bezpredel.versioned.datastore.virtual;

import com.bezpredel.versioned.datastore.Keyed;
import com.bezpredel.versioned.datastore.StorageSystem;
import com.bezpredel.versioned.datastore.UpdateDescriptor;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class VirtualData<DATA, INDX> {
    protected static final Keyed NULL = new Keyed() {
        public Object getKey() {
            return null;
        }
    };

    private Map<DATA, Map<Object, Keyed>> dataMap;
    private Map<INDX, Map<Object, Map<Object, Keyed>>> indexMap;

    private int dataMapEntryCnt = 0;
    private int indexMapEntryCnt = 0;

    public boolean isEmpty() {
        return dataMap==null && indexMap==null;
    }

    public UpdateDescriptor<DATA, INDX> produceUpdateDescriptor() {
        return new UpdateDescriptorImpl<DATA, INDX>(dataMap, indexMap);
    }

    protected Map<Object, Keyed> getDataMap(DATA name) {
        Map<Object, Keyed> retVal;
        if(dataMap==null) {
            dataMap = new HashMap<DATA, Map<Object, Keyed>>();
            retVal = null;
        } else {
            retVal = dataMap.get(name);
        }

        if(retVal==null) {
            retVal = new HashMap<Object, Keyed>();
            dataMap.put(name, retVal);
        }

        return retVal;
    }

    protected Map<Object, Map<Object, Keyed>> getIndexMap(INDX name) {
        Map<Object, Map<Object, Keyed>> retVal;
        if(indexMap==null) {
            indexMap = new HashMap<INDX, Map<Object, Map<Object, Keyed>>>();
            retVal = null;
        } else {
            retVal = indexMap.get(name);
        }

        if(retVal==null) {
            retVal = new HashMap<Object, Map<Object, Keyed>>();
            indexMap.put(name, retVal);
        }
        return retVal;
    }

    protected Keyed _getValue(DATA name, Object key) {
        if (dataMap == null) {
            return null;
        } else {
            Map<Object, Keyed> map = dataMap.get(name);
            if (map == null) {
                return null;
            } else {
                return map.get(key);
            }
        }
    }

    protected boolean _containsKey(DATA name, Object key) {
        if(dataMap==null) {
            return false;
        } else {
            Map<Object, Keyed> map = dataMap.get(name);
            return map!=null && map.containsKey(key);
        }
    }

    protected Map<Object, Keyed> _getValues(DATA name) {
        if(dataMap==null) {
            return Collections.emptyMap();
        } else {
            Map<Object, Keyed> map = dataMap.get(name);
            return map==null ? Collections.<Object, Keyed>emptyMap() : map;
        }
    }

    protected Keyed _put(DATA name, Keyed value) {
        if(value==null) throw new NullPointerException();
        Keyed prevValue = getDataMap(name).put(value.getKey(), value);

        if(prevValue==null) dataMapEntryCnt++;

        return prevValue;
    }

    /**
     *
     * @param name
     * @param key
     * @return @return null in place of NULL
     */
    protected Keyed _remove(DATA name, Object key) {
        Keyed prevValue = getDataMap(name).put(key, NULL);

        if (prevValue == null) dataMapEntryCnt++;

        return prevValue;
    }

    protected Map<Object, Keyed> _getValuesByIndex(INDX name, Object leafKey) {
        if(indexMap==null) {
            return Collections.emptyMap();
        } else {
            Map<Object, Map<Object, Keyed>> map = indexMap.get(name);
            if(map==null) {
                return Collections.emptyMap();
            } else {
                Map<Object, Keyed> leaf = map.get(leafKey);
                return leaf != null ? leaf : Collections.<Object, Keyed>emptyMap();
            }
        }
    }

    protected void _putIntoIndex(INDX name, Object leafKey, Keyed value) {
        if(value==null || leafKey==null) throw new NullPointerException();

        Object key = value.getKey();
        _putIntoIndexInner(name, leafKey, value, key);
    }

    protected void _removeFromIndex(INDX name, Object leafKey, Object key) {
        if(key==null || leafKey==null) throw new NullPointerException();

        _putIntoIndexInner(name, leafKey, NULL, key);
    }

    private void _putIntoIndexInner(INDX name, Object leafKey, Keyed value, Object key) {
        Map<Object, Map<Object, Keyed>> map = getIndexMap(name);
        Map<Object, Keyed> leaf = map.get(leafKey);

        if(leaf==null) {
            leaf = new HashMap<Object, Keyed>();
            map.put(leafKey, leaf);
        }

        Keyed prevValue = leaf.put(key, value);

        if(prevValue==null) indexMapEntryCnt++;
    }


    private static class UpdateDescriptorImpl<DATA,INDX> implements UpdateDescriptor<DATA,INDX> {
        private static final long serialVersionUID = 7452394526921447712L;
        private final Map<DATA, Map<Object, Keyed>> dataMap;
        private final Map<INDX, Map<Object, Map<Object, Keyed>>> indexMap;

        public UpdateDescriptorImpl(Map<DATA, Map<Object, Keyed>> dataMap, Map<INDX, Map<Object, Map<Object, Keyed>>> indexMap) {
            this.dataMap = dataMap;
            this.indexMap = indexMap;
        }

        public String toString() {
            return toString(null);
        }

        public String toString(StorageSystem.ReadContext<DATA, INDX> readContext) {
            StringBuilder sb = new StringBuilder();
            sb.append("UpdateDescriptor");
            if(readContext!=null) sb.append(readContext.getVersion());
            sb.append(":\n");

            if (dataMap != null) {
                for (Map.Entry<DATA, Map<Object, Keyed>> e1 : dataMap.entrySet()) {
                    DATA name = e1.getKey();

                    for (Map.Entry<Object, Keyed> e2 : e1.getValue().entrySet()) {
                        Keyed val = e2.getValue();
                        Object key = e2.getKey();
                        if (val == NULL) {
                            sb.append(name).append('[').append(key).append("] X");
                            if(readContext!=null) {
                                Keyed oldVal = readContext.get(name, key);
                                if(oldVal!=null) {
                                    sb.append(" (from ").append(oldVal).append(")");
                                } else {
                                    sb.append(" (noop)");
                                }
                            }
                            sb.append("\n");
                        } else {
                            sb.append(name).append('[').append(key).append("] ");
                            if(readContext!=null) {
                                Keyed oldVal = readContext.get(name, key);
                                if(oldVal!=null) {
                                    sb.append(oldVal).append(" -> ");
                                } else {
                                    sb.append("null -> ");
                                }
                            }
                            sb.append(val);
                            sb.append("\n");
                        }
                    }
                }
            }

            if (indexMap != null) {
                for (Map.Entry<INDX, Map<Object, Map<Object, Keyed>>> e1 : indexMap.entrySet()) {
                    INDX name = e1.getKey();

                    for (Map.Entry<Object, Map<Object, Keyed>> e2 : e1.getValue().entrySet()) {
                        Object leafKey = e2.getKey();

                        for (Map.Entry<Object, Keyed> e3 : e2.getValue().entrySet()) {
                            Object key = e3.getKey();
                            Keyed val = e3.getValue();
                            if (val == NULL) {
                                sb.append(name).append('[').append(leafKey).append("][").append(key).append("] X\n");
                            } else {
                                sb.append(name).append('[').append(leafKey).append("][").append(key).append("] ").append(val).append("\n");
                            }
                        }
                    }
                }
            }

            return sb.toString();
        }

        public void applyTo(StorageSystem.WriteContext<DATA, INDX> writeContext) {

            if (dataMap != null) {
                for (Map.Entry<DATA, Map<Object, Keyed>> e1 : dataMap.entrySet()) {
                    DATA name = e1.getKey();


                    for (Map.Entry<Object, Keyed> e2 : e1.getValue().entrySet()) {
                        Keyed val = e2.getValue();
                        if (val == NULL) {
                            writeContext.remove(name, e2.getKey());
                        } else {
                            writeContext.put(name, val);
                        }
                    }
                }
            }

            if (indexMap != null) {
                for (Map.Entry<INDX, Map<Object, Map<Object, Keyed>>> e1 : indexMap.entrySet()) {
                    INDX name = e1.getKey();

                    for (Map.Entry<Object, Map<Object, Keyed>> e2 : e1.getValue().entrySet()) {
                        Object leafKey = e2.getKey();

                        for (Map.Entry<Object, Keyed> e3 : e2.getValue().entrySet()) {
                            Keyed val = e3.getValue();
                            if (val == NULL) {
                                writeContext.removeFromIndex(name, leafKey, e3.getKey());
                            } else {
                                writeContext.addToIndex(name, leafKey, val);
                            }
                        }
                    }
                }
            }
        }
    }
}
