package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.HashValue;
import uk.gov.rsf.util.Item;
import uk.gov.rsf.util.Register;

import java.util.Set;

public class LocalAuthorityByTypeIndexFunction extends BaseIndexFunction {
    private final Register register;

    public LocalAuthorityByTypeIndexFunction(Register register) {
        this.register = register;
    }

    @Override
    protected void execute(String key, HashValue itemHash, Set<IndexValueItemPair> result) {
        Item item = register.getItem(itemHash);
        result.add(new IndexValueItemPair(item.getValue("local-authority-type"), item.getSha256hex()));
    }

    @Override
    public String getName() {
        return "local-authority-by-type";
    }
}
