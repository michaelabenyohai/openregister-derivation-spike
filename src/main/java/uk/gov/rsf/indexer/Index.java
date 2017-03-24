package uk.gov.rsf.indexer;

import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.HashValue;
import uk.gov.rsf.util.Register;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This is a structure that stores index data in the below table format:
 *
 * |    index name     | index value | start entry | end entry |   item hash  | start index entry | end index entry |
 * -------------------------------------------------------------------------------------------------------------------
 * | current-countries |      SU     |       1     |      2    |  sha-256:abc |          1        |        2        |
 * |    record         |      SU     |       1     |      2    |  sha-256:abc |          1        |        2        |
 * |    record         |      SU     |       2     |           |  sha-256:dfe |          2        |                 |
 * |    record         |      GB     |       3     |           |  sha-256:ghi |          3        |                 |
 * | current-countries |      GB     |       3     |           |  sha-256:ghi |          3        |                 |
 *
 * When something enters the index, it gets a row with a start entry and a null end entry.
 * When something leaves the index, the end entry gets populated with the entry number at which it no longer exists
 * in the index.
 */
public class Index {
    private List<IndexRow> indexRows;
    private Register register;
    private final Map<String, Integer> currentEntryNumbers = new HashMap<>();

    public Index(Register register) {
        this.register = register;
        this.indexRows = new ArrayList<>();
    }

    public void updateValuesForIndex(List<IndexValueItemPair> toStart, List<IndexValueItemPair> toEnd, String indexName, String key, int entryNumber) {
        Map<String, List<IndexValueItemPairEvent>> valueChanges = Stream.concat(
                toStart.stream().map(p -> new IndexValueItemPairEvent(p, true)),
                toEnd.stream().map(p -> new IndexValueItemPairEvent(p, false)))
                .collect(Collectors.groupingBy(IndexValueItemPairEvent::getIndexValue));

        if (!currentEntryNumbers.containsKey(indexName)) {
            currentEntryNumbers.put(indexName, 0);
        }

        int currentMaxIndexEntry = currentEntryNumbers.get(indexName);
        for (Map.Entry<String, List<IndexValueItemPairEvent>> v : valueChanges.entrySet().stream().sorted(Comparator.comparing(Map.Entry::getKey)).collect(Collectors.toList())) {
            int thisIndexEntryNumber = currentMaxIndexEntry + 1;
            for (IndexValueItemPairEvent h : v.getValue()) {
                List<IndexRow> rows = getCurrentRowsForIndexValue(indexName, Optional.of(h.getIndexValue())).collect(Collectors.toList());
                if (h.isStart()) {
                    if (rows.stream().noneMatch(row -> row.getItemHash().equals(h.getItemHash()))) {
                        indexRows.add(new IndexRow(indexName, h.getIndexValue(), h.getItemHash(), entryNumber, thisIndexEntryNumber));
                        currentMaxIndexEntry = thisIndexEntryNumber;
                    } else {
                        indexRows.add(new IndexRow(indexName, h.getIndexValue(), h.getItemHash(), entryNumber));
                    }
                } else {
                    List<IndexRow> rows2 = rows.stream().filter(row -> row.getItemHash().equals(h.getItemHash())).collect(Collectors.toList());
                    if (rows2.size() == 1) {
                        setEndIndexEntryForIndex(indexName, h.getIndexValue(), h.getItemHash(), key, thisIndexEntryNumber);
                        currentMaxIndexEntry = thisIndexEntryNumber;
                    }
                    setEndEntryForIndex(indexName, h.getIndexValue(), h.getItemHash(), key, entryNumber);
                }
            }
        }

        currentEntryNumbers.put(indexName, currentMaxIndexEntry);
    }

    public void setEndEntryForIndex(String indexName, String indexValue, HashValue itemHash, String key, int endEntryNumber) {
        getCurrentRowsForIndexValue(indexName, Optional.of(indexValue))
                .filter(row -> row.getItemHash().equals(itemHash))
                .filter(row -> register.getEntry(row.getStartEntry()).getKey().equals(key))
                .forEach(toEnd -> toEnd.setEndEntry(endEntryNumber));
    }

    public void setEndIndexEntryForIndex(String indexName, String indexValue, HashValue itemHash, String key, int endIndexEntryNumber) {
        getCurrentRowsForIndexValue(indexName, Optional.of(indexValue))
                .filter(row -> row.getItemHash().equals(itemHash))
                .filter(row -> register.getEntry(row.getStartEntry()).getKey().equals(key))
                .forEach(toEnd -> toEnd.setEndIndexEntry(endIndexEntryNumber));
    }

    public List<Entry> getCurrentItemsForIndex(String indexName, Optional<String> indexValue, Optional<Integer> registerVersion) {
        Map<String, Set<HashValue>> currentIndexRows = (registerVersion.isPresent()
                ? getCurrentRowsWithIndexEntryForIndexValueAtVersion(indexName, indexValue, registerVersion.get())
                : getCurrentRowsWithIndexEntryForIndexValue(indexName, indexValue)).collect(Collectors.groupingBy(row -> row.getValue(), Collectors.mapping(row -> row.getItemHash(), Collectors.toSet())));

        List<IndexRow> allIndexRows = (registerVersion.isPresent()
                ? getAllRowsForIndexValueAtVersion(indexName, indexValue, registerVersion.get())
                : getAllRowsForIndexValue(indexName, indexValue)).collect(Collectors.toList());

        List<Entry> entries = new ArrayList<>();
        for (Map.Entry<String, Set<HashValue>> me : currentIndexRows.entrySet()) {
            IndexValueEvent entryEvent = getAllIndexValueEvents(allIndexRows.stream().filter(row -> row.getValue().equals(me.getKey())).collect(Collectors.toList()), registerVersion)
                    .collect(Collectors.maxBy(Comparator.comparing(IndexValueEvent::getIndexEntryNumber))).get();

            Entry entry = new Entry(entryEvent.getIndexEntryNumber(), entryEvent.getEntryNumber(), me.getValue(), register.getEntry(entryEvent.getEntryNumber()).getTimestamp(), me.getKey());
            entries.add(entry);
        }

        return entries;
    }

    public Set<HashValue> getCurrentItemsForIndexValue(String indexName, String indexValue, Optional<Integer> registerVersion) {
        Stream<IndexRow> indexValueRows = registerVersion.isPresent()
                ? getCurrentRowsWithIndexEntryForIndexValueAtVersion(indexName, Optional.of(indexValue), registerVersion.get())
                : getCurrentRowsWithIndexEntryForIndexValue(indexName, Optional.of(indexValue));
        return indexValueRows.map(row -> row.getItemHash()).collect(Collectors.toSet());
    }

    public Map<IndexRow.IndexValueEntryNumberPair, Set<HashValue>> getAllItemsByIndexValueAndOriginalEntryNumber(String indexName, Optional<String> indexValue, Optional<Integer> registerVersion) {
        List<IndexRow> allRowsForIndexValue = (registerVersion.isPresent()
                ? getAllRowsForIndexValueAtVersion(indexName, indexValue, registerVersion.get())
                : getAllRowsForIndexValue(indexName, indexValue))
                .collect(Collectors.toList());

        Stream<IndexValueEvent> indexValueEvents = getAllIndexValueEvents(allRowsForIndexValue, registerVersion);

        return getItemsForIndexValueEntryNumberPair(indexValueEvents);
    }

    private Map<IndexRow.IndexValueEntryNumberPair, Set<HashValue>> getItemsForIndexValueEntryNumberPair(Stream<IndexValueEvent> indexValueEvents) {
        Map<IndexRow.IndexValueEntryNumberPair, Set<HashValue>> itemsForEachIndexValueAtOriginalEntryNumber = new HashMap<>();
        indexValueEvents.forEach(indexValueEvent -> {
            Set<HashValue> hashes = getCurrentItemsForIndexValue(indexValueEvent.getOriginalIndexRow().getName(), indexValueEvent.getOriginalIndexRow().getValue(), Optional.of(indexValueEvent.getOriginalIndexRow().getTransaction(indexValueEvent.isStartEvent()).getEntryNumber()));
            itemsForEachIndexValueAtOriginalEntryNumber.put(indexValueEvent.getOriginalIndexRow().getTransaction(indexValueEvent.isStartEvent()), hashes);
        });
        return itemsForEachIndexValueAtOriginalEntryNumber;
    }

    private Stream<IndexValueEvent> getAllIndexValueEvents(List<IndexRow> indexValueRows, Optional<Integer> registerVersion) {
        return Stream.concat(
                indexValueRows.stream()
                        .filter(r -> !registerVersion.isPresent() || r.getStartEntry() <= registerVersion.get())
                        .filter(r -> r.getStartIndexEntry().isPresent())
                        .map(r -> new IndexValueEvent(r, true)),
                indexValueRows.stream()
                        .filter(r -> !r.isCurrent() && (!registerVersion.isPresent() || r.getEndEntry() <= registerVersion.get()))
                        .filter(r -> r.getEndIndexEntry().isPresent())
                        .map(r -> new IndexValueEvent(r, false)));
    }

    // gets all rows for an index name and value that are current at a particular register-entry-number and do have a corresponding index-entry (start or end)
    private Stream<IndexRow> getCurrentRowsWithIndexEntryForIndexValueAtVersion(String indexName, Optional<String> indexValue, int version) {
        return getAllRowsForIndexValue(indexName, indexValue)
                .filter(row -> (row.isCurrent() && row.getStartEntry() <= version && row.getStartIndexEntry().isPresent())
                        || (!row.isCurrent() && row.getStartEntry() <= version && row.getEndEntry() > version) && row.getStartIndexEntry().isPresent());
    }

    // gets all rows for an index name and value that are current (do not have a populated end-entry) and do have a corresponding index-entry (start)
    private Stream<IndexRow> getCurrentRowsWithIndexEntryForIndexValue(String indexName, Optional<String> indexValue) {
        return getAllRowsForIndexValue(indexName, indexValue)
                .filter(row -> row.isCurrent() && row.getStartIndexEntry().isPresent());
    }

    // gets all rows for an index name and value that are current (does not have a populated end-entry)
    private Stream<IndexRow> getCurrentRowsForIndexValue(String indexName, Optional<String> indexValue) {
        return getAllRowsForIndexValue(indexName, indexValue)
                .filter(row -> row.isCurrent());
    }

    // gets all rows for an index name and value at a particular register-entry-number
    private Stream<IndexRow> getAllRowsForIndexValueAtVersion(String indexName, Optional<String> indexValue, int version) {
        return  getAllRowsForIndexValue(indexName, indexValue)
                .filter(ir -> ((ir.isCurrent() && ir.getStartEntry() <= version) || (!ir.isCurrent() && ir.getStartEntry() <= version)));
    }

    // gets all rows for an index name and value
    private Stream<IndexRow> getAllRowsForIndexValue(String indexName, Optional<String> indexValue) {
        return indexRows.stream()
                .filter(row -> row.getName().equals(indexName))
                .filter(row -> !indexValue.isPresent() || row.getValue().equals(indexValue.get()));
    }

    @Override
    public String toString() {
        return String.join("\n", indexRows.stream().map(IndexRow::toString).collect(Collectors.toList()));
    }

    private class IndexValueEvent {
        public IndexRow originalIndexRow;
        public boolean start;

        public IndexValueEvent(IndexRow indexRow, boolean start) {
            this.originalIndexRow = indexRow;
            this.start = start;
        }

        public IndexRow getOriginalIndexRow() {
            return originalIndexRow;
        }

        public boolean isStartEvent() {
            return  start;
        }

        public int getIndexEntryNumber() {
            return start ? originalIndexRow.getStartIndexEntry().get() : originalIndexRow.getEndIndexEntry().get();
        }

        public int getEntryNumber() {
            return start ? originalIndexRow.getStartEntry() : originalIndexRow.getEndEntry();
        }
    }

    private class IndexValueItemPairEvent {
        private final IndexValueItemPair pair;
        private final boolean isStart;

        public IndexValueItemPairEvent(IndexValueItemPair pair, boolean isStart) {
            this.pair = pair;
            this.isStart = isStart;
        }

        public HashValue getItemHash() {
            return pair.getItemHash();
        }

        public String getIndexValue() {
            return pair.getValue();
        }

        public boolean isStart() {
            return isStart;
        }
    }
}


