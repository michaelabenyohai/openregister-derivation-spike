package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.HashValue;

import java.util.HashSet;
import java.util.Set;

public abstract class BaseIndexFunction implements IndexFunction {

    @Override
    public Set<IndexValueItemPair> execute(Entry entry) {
        Set<IndexValueItemPair> result = new HashSet<>();

        entry.getSha256hex().forEach(itemHash -> {
            execute(entry.getKey(), itemHash, result);
        });

        return result;
    }

    protected abstract void execute(String key, HashValue itemHash, Set<IndexValueItemPair> result);
}
