package uk.gov.rsf.indexer;

import uk.gov.rsf.indexer.function.IndexFunction;
import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.Register;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class IndexDriver {

    private final Index index;
    private final Register register;

    public IndexDriver(Register register) {
        this.index = new Index(register);
        this.register = register;
    }

    public void indexEntry(Entry entry, IndexFunction indexFunction) {
        Optional<Entry> currentRecord = register.getRecord(entry.getKey());
        Set<IndexValueItemPair> currentIndexValueItemPairs = new HashSet<>();
        if (currentRecord.isPresent()) {
            currentIndexValueItemPairs.addAll(indexFunction.execute(currentRecord.get()));
        }

        Set<IndexValueItemPair> newIndexValueItemPairs = indexFunction.execute(entry);

        endIndices(currentIndexValueItemPairs, newIndexValueItemPairs, indexFunction.getName(), entry);
        startIndices(currentIndexValueItemPairs, newIndexValueItemPairs, indexFunction.getName(), entry);
    }

    private void endIndices(Set<IndexValueItemPair> start, Set<IndexValueItemPair> end, String indexName, Entry entry) {
        List<IndexValueItemPair> toEnd = start.stream().filter(i -> !end.contains(i)).collect(Collectors.toList());
        toEnd.forEach(i -> index.endIndex(indexName, i.getValue(), i.getItemHash(), entry));
    }

    private void startIndices(Set<IndexValueItemPair> start, Set<IndexValueItemPair> end, String indexName, Entry entry) {
        List<IndexValueItemPair> toStart = end.stream().filter(i -> !start.contains(i)).collect(Collectors.toList());
        toStart.forEach(i -> index.start(indexName, i.getValue(), entry.getEntryNumber(), i.getItemHash()));
    }

    public Index getIndex() {
        return index;
    }
}
