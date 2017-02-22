package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.Entry;

import java.util.Set;
import java.util.stream.Collectors;

public class RecordIndexFunction implements IndexFunction {

    public RecordIndexFunction() {}

    @Override
    public Set<IndexValueItemPair> execute(Entry entry) {
        return entry.getSha256hex().stream()
                .map(hashValue -> new IndexValueItemPair(entry.getKey(), hashValue))
                .collect(Collectors.toSet());
    }

    @Override
    public String getName() {
        return "record";
    }
}
