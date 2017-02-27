package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.HashValue;

import java.util.Set;

public class RecordIndexFunction extends BaseIndexFunction {

    public RecordIndexFunction() {}

    @Override
    protected void execute(String key, HashValue itemHash, Set<IndexValueItemPair> result) {
        result.add(new IndexValueItemPair(key, itemHash));
    }

    @Override
    public String getName() {
        return "record";
    }
}
