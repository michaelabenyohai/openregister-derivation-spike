package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.Entry;

import java.util.Set;

public interface IndexFunction {
    Set<IndexValueItemPair> execute(Entry entry);
    String getName();
}
