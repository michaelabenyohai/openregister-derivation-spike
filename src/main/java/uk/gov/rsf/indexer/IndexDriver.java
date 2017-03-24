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

        List<IndexValueItemPair> toEnd = getEndIndices(currentIndexValueItemPairs, newIndexValueItemPairs);
        List<IndexValueItemPair> toStart = getStartIndices(currentIndexValueItemPairs, newIndexValueItemPairs);

        index.updateValuesForIndex(toStart, toEnd, indexFunction.getName(), entry.getKey(), entry.getEntryNumber());
    }

    private List<IndexValueItemPair> getEndIndices(Set<IndexValueItemPair> start, Set<IndexValueItemPair> end) {
        return start.stream().filter(i -> !end.contains(i)).collect(Collectors.toList());
    }

    private List<IndexValueItemPair> getStartIndices(Set<IndexValueItemPair> start, Set<IndexValueItemPair> end) {
        return end.stream().filter(i -> !start.contains(i)).collect(Collectors.toList());
    }

    public Index getIndex() {
        return index;
    }
}
