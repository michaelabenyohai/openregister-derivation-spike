package uk.gov.rsf.indexer.function;

import uk.gov.rsf.indexer.IndexValueItemPair;
import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.Item;
import uk.gov.rsf.util.Register;

import java.util.HashSet;
import java.util.Set;

public class SchoolByAgeIndexFunction implements IndexFunction {
    private final Register register;

    public SchoolByAgeIndexFunction(Register register) {
        this.register = register;
    }

    @Override
    public Set<IndexValueItemPair> execute(Entry entry) {
        Set<IndexValueItemPair> indexValueItemPairs = new HashSet<>();
        Item item = register.getItem(entry.getSha256hex().get(0));

        if (!(item.getFieldsStream().noneMatch(f -> f.getKey().equals("minimum-age")) || item.getFieldsStream().noneMatch(f -> f.getKey().equals("maximum-age")))) {
            int minAge = Integer.parseInt(item.getValue("minimum-age"));
            int maxAge = Integer.parseInt(item.getValue("maximum-age"));

            for (int i = minAge; i <= maxAge; i++) {
                indexValueItemPairs.add(new IndexValueItemPair(Integer.toString(i), item.getSha256hex()));
            }
        }

        return indexValueItemPairs;
    }

    @Override
    public String getName() {
        return "school-by-age";
    }
}
