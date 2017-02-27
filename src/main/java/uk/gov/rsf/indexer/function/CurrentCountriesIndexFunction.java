package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.HashValue;
import uk.gov.rsf.util.Item;
import uk.gov.rsf.util.Register;

import java.util.HashSet;
import java.util.Set;

public class CurrentCountriesIndexFunction extends BaseIndexFunction {
    private final Register register;

    public CurrentCountriesIndexFunction(Register register) {
        this.register = register;
    }

    @Override
    protected void execute(String key, HashValue itemHash, Set<IndexValueItemPair> result) {
        Item item = register.getItem(itemHash);
        if (!item.getFieldsStream().anyMatch(f -> f.getKey().equals("end-date"))) {
            result.add(new IndexValueItemPair(key, item.getSha256hex()));
        }
    }

    @Override
    public String getName() {
        return "current-countries";
    }
}
