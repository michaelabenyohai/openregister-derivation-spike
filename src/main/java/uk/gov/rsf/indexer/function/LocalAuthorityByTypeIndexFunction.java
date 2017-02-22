package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.Item;
import uk.gov.rsf.util.Register;

import java.util.HashSet;
import java.util.Set;

public class LocalAuthorityByTypeIndexFunction implements IndexFunction {
    private Register register;

    public LocalAuthorityByTypeIndexFunction(Register register) {
        this.register = register;
    }

    @Override
    public Set<IndexValueItemPair> execute(Entry entry) {
        Set<IndexValueItemPair> result = new HashSet<>();
        Item item = register.getItem(entry.getSha256hex().get(0));

        result.add(new IndexValueItemPair(item.getValue("local-authority-type"), item.getSha256hex()));
        return result;
    }

    @Override
    public String getName() {
        return "local-authority-by-type";
    }
}
