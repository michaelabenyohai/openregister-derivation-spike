package uk.gov.rsf.indexer;

import uk.gov.rsf.util.HashValue;

import java.util.Optional;

public class IndexRow {
    private final String name;
    private final String value;
    private final int startEntry;
    private Optional<Integer> endEntry;
    private final HashValue itemHash;

    public IndexRow(String name, String value, int startEntry, HashValue itemHash) {
        this.name = name;
        this.value = value;
        this.startEntry = startEntry;
        this.endEntry = Optional.empty();
        this.itemHash = itemHash;
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public int getStartEntry() {
        return startEntry;
    }

    public boolean isCurrent() {
        return !endEntry.isPresent();
    }

    public void setEndEntry(int value) {
        endEntry = Optional.of(value);
    }

    public Integer getEndEntry() {
        return endEntry.orElseGet(null);
    }

    public HashValue getItemHash() {
        return itemHash;
    }

    public IndexValueEvent getTransaction(boolean start) {
        return new IndexValueEvent(start ? startEntry : endEntry.get(), value);
    }

    @Override
    public String toString() {
        return String.join("\t", name, value, Integer.toString(startEntry), endEntry.isPresent() ? endEntry.get().toString() : "null", itemHash.toString());
    }

    public class IndexValueEvent {
        private final int originalEntryNumber;
        private final String indexValue;

        public IndexValueEvent(int originalEntryNumber, String indexValue) {
            this.originalEntryNumber = originalEntryNumber;
            this.indexValue = indexValue;
        }

        public int getOriginalEntryNumber() {
            return originalEntryNumber;
        }

        public String getIndexValue() {
            return indexValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IndexValueEvent obj = (IndexValueEvent) o;

            if (originalEntryNumber != obj.originalEntryNumber) return false;
            return indexValue == null ? obj.indexValue == null : indexValue.equals(obj.indexValue);
        }

        @Override
        public int hashCode() {
            int result = indexValue != null ? indexValue.hashCode() : 0;
            result = 31 * originalEntryNumber + result;
            return result;
        }
    }
}
