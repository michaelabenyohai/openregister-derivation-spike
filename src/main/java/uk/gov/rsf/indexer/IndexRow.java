package uk.gov.rsf.indexer;

import uk.gov.rsf.util.HashValue;

import java.util.Optional;

public class IndexRow {
    private final String name;
    private final String value;
    private final HashValue itemHash;
    private final int startEntry;
    private Optional<Integer> endEntry;
    private final Optional<Integer> startIndexEntry;
    private Optional<Integer> endIndexEntry;

    public IndexRow(String name, String value, HashValue itemHash, int startEntry) {
        this(name, value, itemHash, startEntry, Optional.empty());
    }

    public IndexRow(String name, String value, HashValue itemHash, int startEntry, int startIndexEntry) {
        this(name, value, itemHash, startEntry, Optional.of(startIndexEntry));
    }

    private IndexRow(String name, String value, HashValue itemHash, int startEntry, Optional<Integer> startIndexEntry) {
        this.name = name;
        this.value = value;
        this.itemHash = itemHash;
        this.startEntry = startEntry;
        this.endEntry = Optional.empty();
        this.startIndexEntry = startIndexEntry;
        this.endIndexEntry = Optional.empty();
    }

    public String getName() {
        return name;
    }

    public String getValue() {
        return value;
    }

    public HashValue getItemHash() {
        return itemHash;
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

    public Optional<Integer> getStartIndexEntry() {
        return startIndexEntry;
    }

    public void setEndIndexEntry(int value) {
        endIndexEntry = Optional.of(value);
    }

    public Optional<Integer> getEndIndexEntry() {
        return endIndexEntry;
    }

    public IndexValueEntryNumberPair getTransaction(boolean start) {
        return new IndexValueEntryNumberPair(start ? startEntry : endEntry.get(), value);
    }

    @Override
    public String toString() {
        return String.join("\t", name, value, itemHash.toString(), Integer.toString(startEntry),
                endEntry.isPresent() ? endEntry.get().toString() : "null",
                startIndexEntry.isPresent() ? startIndexEntry.get().toString() : "null",
                endIndexEntry.isPresent() ? endIndexEntry.get().toString() : "null");
    }

    public class IndexValueEntryNumberPair {
        private final int entryNumber;
        private final String indexValue;

        public IndexValueEntryNumberPair(int entryNumber, String indexValue) {
            this.entryNumber = entryNumber;
            this.indexValue = indexValue;
        }

        public int getEntryNumber() {
            return entryNumber;
        }

        public String getIndexValue() {
            return indexValue;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            IndexValueEntryNumberPair obj = (IndexValueEntryNumberPair) o;

            if (entryNumber != obj.entryNumber) return false;
            return indexValue == null ? obj.indexValue == null : indexValue.equals(obj.indexValue);
        }

        @Override
        public int hashCode() {
            int result = indexValue != null ? indexValue.hashCode() : 0;
            result = 31 * entryNumber + result;
            return result;
        }
    }
}
