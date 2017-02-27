package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.HashValue;
import uk.gov.rsf.util.Item;
import uk.gov.rsf.util.Register;

import java.util.Set;

public class SchoolByAgeIndexFunction extends BaseIndexFunction {
    private final Register register;

    public SchoolByAgeIndexFunction(Register register) {
        this.register = register;
    }

    @Override
    protected void execute(String key, HashValue itemHash, Set<IndexValueItemPair> result) {
        Item item = register.getItem(itemHash);
        if (!(item.getFieldsStream().noneMatch(f -> f.getKey().equals("minimum-age")) || item.getFieldsStream().noneMatch(f -> f.getKey().equals("maximum-age")))) {
            int minAge = Integer.parseInt(item.getValue("minimum-age"));
            int maxAge = Integer.parseInt(item.getValue("maximum-age"));

            for (int i = minAge; i <= maxAge; i++) {
                result.add(new IndexValueItemPair(Integer.toString(i), item.getSha256hex()));
            }
        }
    }

    @Override
    public String getName() {
        return "school-by-age";
    }
}
