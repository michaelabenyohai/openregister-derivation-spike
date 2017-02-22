package uk.gov.rsf.indexer;

import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.HashValue;
import uk.gov.rsf.util.Register;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Index {
    private List<IndexRow> indexRows;

    public Index(Register register) {
        this.indexRows = new ArrayList<>();
    }

    public void start(String name, String value, int entryStart, HashValue itemHash) {
        indexRows.add(new IndexRow(name, value, entryStart, itemHash));
    }

    public void endIndex(String name, String value, HashValue itemHash, Entry entry) {
        Optional<IndexRow> toEnd = indexRows.stream().filter(indexRow -> indexRow.getName().equals(name)
                && indexRow.getItemHash().equals(itemHash)
                && indexRow.getValue().equals(value)
                && indexRow.isCurrent())
                .findFirst();

        toEnd.get().setEndEntry(entry.getEntryNumber());
    }

    public Map<String, List<HashValue>> getCurrentItemsForIndex(String indexName, Optional<String> indexValue, Optional<Integer> registerVersion) {
        Stream<IndexRow> indexValueRows = registerVersion.isPresent()
                ? getCurrentRowsForIndexValueAtVersion(indexName, indexValue, registerVersion.get())
                : getCurrentRowsForIndexValue(indexName, indexValue);

        return indexValueRows.collect(Collectors.groupingBy(row -> row.getValue(), Collectors.mapping(row -> row.getItemHash(), Collectors.toList())));
    }

    public List<HashValue> getCurrentItemsForIndexValue(String indexName, String indexValue, Optional<Integer> registerVersion) {
        Stream<IndexRow> indexValueRows = registerVersion.isPresent()
                ? getCurrentRowsForIndexValueAtVersion(indexName, Optional.of(indexValue), registerVersion.get())
                : getCurrentRowsForIndexValue(indexName, Optional.of(indexValue));
        return indexValueRows.map(row -> row.getItemHash()).collect(Collectors.toList());
    }

    private Stream<IndexRow> getAllRowsForIndexValueAtVersion(String indexName, Optional<String> indexValue, int version) {
        return  getRowsForIndexValue(indexName, indexValue)
                .filter(ir -> ((ir.isCurrent() && ir.getStartEntry() <= version) || (!ir.isCurrent() && ir.getStartEntry() <= version)));
    }

    private Stream<IndexRow> getCurrentRowsForIndexValueAtVersion(String indexName, Optional<String> indexValue, int version) {
        return getRowsForIndexValue(indexName, indexValue)
                .filter(row -> (row.isCurrent() && row.getStartEntry() <= version)
                        || (!row.isCurrent() && row.getStartEntry() <= version && row.getEndEntry() > version));
    }

    private Stream<IndexRow> getCurrentRowsForIndexValue(String indexName, Optional<String> indexValue) {
        return getRowsForIndexValue(indexName, indexValue)
                .filter(row -> row.isCurrent());
    }

    private Stream<IndexRow> getRowsForIndexValue(String indexName, Optional<String> indexValue) {
        return indexRows.stream()
                .filter(row -> row.getName().equals(indexName))
                .filter(row -> !indexValue.isPresent() || row.getValue().equals(indexValue.get()));
    }

    public Map<IndexRow.IndexValueEvent, List<HashValue>> getItemsByIndexValueAndOriginalEntryNumber(String indexName, Optional<String> indexValue, Optional<Integer> registerVersion) {
        List<IndexRow> indexValueRows = (registerVersion.isPresent()
                ? getAllRowsForIndexValueAtVersion(indexName, indexValue, registerVersion.get())
                : getRowsForIndexValue(indexName, indexValue))
                .collect(Collectors.toList());

        Stream<StartEndAction> startEndActions = Stream.concat(
                indexValueRows.stream()
                        .filter(r -> !registerVersion.isPresent() || r.getStartEntry() <= registerVersion.get())
                        .map(r -> new StartEndAction(r, true)),
                indexValueRows.stream()
                        .filter(r -> !r.isCurrent() && (!registerVersion.isPresent() || r.getEndEntry() <= registerVersion.get()))
                        .map(r -> new StartEndAction(r, false)));

        Map<IndexRow.IndexValueEvent, List<HashValue>> bla = new HashMap<>();
        startEndActions.forEach(startEndAction -> {
                List<HashValue> hashes = getCurrentItemsForIndexValue(startEndAction.getOriginalIndexRow().getName(), startEndAction.getOriginalIndexRow().getValue(), Optional.of(startEndAction.getOriginalIndexRow().getTransaction(startEndAction.isStart()).getOriginalEntryNumber()));
                bla.put(startEndAction.getOriginalIndexRow().getTransaction(startEndAction.isStart()), hashes);
        });
        return bla;
    }

    @Override
    public String toString() {
        return String.join("\n", indexRows.stream().map(IndexRow::toString).collect(Collectors.toList()));
    }

    private class StartEndAction {
        public IndexRow originalIndexRow;
        public boolean start;

        public StartEndAction(IndexRow indexRow, boolean start) {
            this.originalIndexRow = indexRow;
            this.start = start;
        }

        public IndexRow getOriginalIndexRow() {
            return originalIndexRow;
        }

        public boolean isStart() {
            return  start;
        }
    }
}


