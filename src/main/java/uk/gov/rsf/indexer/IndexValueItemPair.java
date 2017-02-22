package uk.gov.rsf.indexer;

import uk.gov.rsf.util.HashValue;

public class IndexValueItemPair {
    private final String value;
    private final HashValue itemHash;

    public IndexValueItemPair(String value, HashValue itemHash) {
        this.value = value;
        this.itemHash = itemHash;
    }

    public String getValue() {
        return value;
    }

    public HashValue getItemHash() {
        return itemHash;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || o.getClass() != this.getClass()) return false;

        IndexValueItemPair obj = (IndexValueItemPair) o;

        if (value != null ? !value.equals(obj.value) : obj.value != null) return false;

        return itemHash != null ? itemHash.equals(obj.itemHash) : obj.itemHash == null;
    }

    @Override
    public int hashCode() {
        int result = value != null ? value.hashCode() : 0;
        result = 31 * result + (itemHash != null ? itemHash.hashCode() : 0);
        return result;
    }
}
