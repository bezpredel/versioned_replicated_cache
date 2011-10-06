package com.bezpredel.versioned.example.data;

import com.bezpredel.versioned.cache.AbstractImmutableCacheableObject;
import com.bezpredel.versioned.cache.BasicCacheIdentifier;
import com.bezpredel.versioned.cache.BasicOneToManyIndexIdentifier;

public class User extends AbstractImmutableCacheableObject {
    public static final BasicCacheIdentifier CACHE = new BasicCacheIdentifier("User", User.class);
    public static final BasicOneToManyIndexIdentifier BY_INSTITUTION = new BasicOneToManyIndexIdentifier(CACHE, "institution");

    private static final long serialVersionUID = -6209323010130913787L;

    private String userName;
    private String firstName;
    private String lastName;
    private Object institutionId;
    private boolean active;

    public User(Object key) {
        super(key);
    }

    public String getUserName() {
        return userName;
    }

    public Object getInstitutionId() {
        return institutionId;
    }

    public boolean isActive() {
        return active;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public void setFirstName(String firstName) {
        checkIfModificationIsAllowed();
        this.firstName = firstName;
    }

    public void setLastName(String lastName) {
        checkIfModificationIsAllowed();
        this.lastName = lastName;
    }

    public void setInstitutionId(Object institutionId) {
        checkIfModificationIsAllowed();
        this.institutionId = institutionId;
    }

    public void setInstitution(Institution institution) {
        checkIfModificationIsAllowed();
        this.institutionId = institution==null ? null : institution.getKey();
    }

    public void setUserName(String userName) {
        checkIfModificationIsAllowed();

        this.userName = userName;
    }

    public void setActive(boolean active) {
        checkIfModificationIsAllowed();

        this.active = active;
    }



    public BasicCacheIdentifier getCacheType() {
        return CACHE;
    }


}
