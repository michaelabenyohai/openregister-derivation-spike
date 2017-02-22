package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.Item;
import uk.gov.rsf.util.Register;

import java.util.HashSet;
import java.util.Set;

public class CurrentCountriesIndexFunction implements IndexFunction {
    private final Register register;

    public CurrentCountriesIndexFunction(Register register) {
        this.register = register;
    }

    @Override
    public Set<IndexValueItemPair> execute(Entry entry) {
        Set<IndexValueItemPair> indexValueItemPairs = new HashSet<>();
        Item item = register.getItem(entry.getSha256hex().get(0));

        if (!item.getFieldsStream().anyMatch(f -> f.getKey().equals("end-date"))) {
            indexValueItemPairs.add(new IndexValueItemPair(entry.getKey(), item.getSha256hex()));
        }
        return indexValueItemPairs;
    }

    @Override
    public String getName() {
        return "current-countries";
    }
}
