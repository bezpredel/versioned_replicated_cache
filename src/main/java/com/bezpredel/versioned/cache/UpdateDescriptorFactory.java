//package com.bezpredel.versioned.cache;
//
//import com.bezpredel.versioned.datastore.Keyed;
//import com.bezpredel.versioned.datastore.StorageSystem;
//import com.bezpredel.versioned.datastore.UpdateDescriptor;
//
//import java.util.ArrayList;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//
//public class UpdateDescriptorFactory {
//    private static final UpdateDescriptor.ApplyFilter<OneToOneID, OneToManyID> FILTER = new UpdateDescriptor.ApplyFilter<OneToOneID, OneToManyID>() {
//        public boolean acceptData(OneToOneID oneToOneID) {
//            return !oneToOneID.isIndex();
//        }
//
//        public boolean acceptIndex(OneToManyID data) {
//            return false;
//        }
//    };
//
//    public HighLevelUpdateDescriptor createUpdateDescriptor(UpdateDescriptor<OneToOneID, OneToManyID> source) {
//        CapturingContextImpl cc = new CapturingContextImpl();
//        source.applyTo(cc, FILTER);
//        return new UpdateDescriptorImpl(cc);
//    }
//
//    private static class UpdateDescriptorImpl implements HighLevelUpdateDescriptor {
//        private static final long serialVersionUID = 9107690910557430995L;
//        private List<ImmutableCacheableObject<BasicCacheIdentifier>> added;
//        private Map<OneToOneID, List<Object>> removedKeys;
//
//        private UpdateDescriptorImpl(CapturingContextImpl capturingContext) {
//            added = capturingContext.added;
//            removedKeys = capturingContext.removedKeys;
//        }
//
//        public void applyTo(CacheService.WriteContext writeContext) {
//            if(removedKeys!=null) {
//                for(Map.Entry<OneToOneID, List<Object>> entry : removedKeys.entrySet()) {
//                    BasicCacheIdentifier cache = entry.getKey().getCacheIdentifier();
//                    for(Object key : entry.getValue()) {
//                        writeContext.remove(cache, key);
//                    }
//                }
//            }
//
//            if(added!=null) {
//                for(ImmutableCacheableObject<BasicCacheIdentifier> obj : added) {
//                    writeContext.put(obj);
//                }
//
//            }
//        }
//    }
//
//    private static class CapturingContextImpl implements StorageSystem.WriteContext<OneToOneID, OneToManyID> {
//        private List<ImmutableCacheableObject<BasicCacheIdentifier>> added;
//        private Map<OneToOneID, List<Object>> removedKeys;
//
//        private List<Object> getRemovedKeys(OneToOneID id) {
//            List<Object> list = removedKeys.get(id);
//            if(list==null) {
//                removedKeys.put(id, list = new ArrayList<Object>());
//            }
//            return list;
//        }
//
//        private List<ImmutableCacheableObject<BasicCacheIdentifier>> getAddedObjects() {
//            if(added==null) added = new ArrayList<ImmutableCacheableObject<BasicCacheIdentifier>>();
//            return added;
//        }
//
//        public <T extends Keyed> Iterator<T> valuesByIndex(OneToManyID name, Object leafKey) {
//            throw new UnsupportedOperationException();
//        }
//
//        public <T extends Keyed> Iterator<T> values(OneToOneID name) {
//            throw new UnsupportedOperationException();
//        }
//
//        public <T extends Keyed> T get(OneToOneID name, Object key) {
//            throw new UnsupportedOperationException();
//        }
//
//        public int getVersion() {
//            throw new UnsupportedOperationException();
//        }
//
//        public void removeFromIndex(OneToManyID name, Object leafKey, Object objectKey) {
//            throw new UnsupportedOperationException();
//        }
//
//        public <T extends Keyed> void addToIndex(OneToManyID name, Object leafKey, T value) {
//            throw new UnsupportedOperationException();
//        }
//
//        public <T extends Keyed> T remove(OneToOneID name, Object key) {
//            getRemovedKeys(name).add(key);
//            return null;
//        }
//
//        public <T extends Keyed> T put(OneToOneID name, T value) {
//            getAddedObjects().add((ImmutableCacheableObject<BasicCacheIdentifier>) value);
//            return null;
//        }
//    }
//}
