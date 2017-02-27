package uk.gov.rsf.serialization;

import uk.gov.rsf.indexer.function.*;
import uk.gov.rsf.serialization.handlers.AddItemCommandHandler;
import uk.gov.rsf.serialization.handlers.AppendEntryCommandHandler;
import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.HashValue;
import uk.gov.rsf.util.Register;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

public class RSFService {

    private static final List<Function<Register, IndexFunction>> availableIndexFunctions = Arrays.asList(
            r -> new CurrentCountriesIndexFunction(r),
            r -> new LocalAuthorityByTypeIndexFunction(r),
            r -> new SchoolByAgeIndexFunction(r));

    private static final List<IndexFunction> defaultIndexFunctions = Arrays.asList(
            new RecordIndexFunction());

    public static void morc(InputStream inputStream, String indexName, String indexRender, Optional<String> indexValue, Optional<Integer> registerVersion) {
        Register register = new Register();
        if (!indexName.equals("indexTable")
                && !defaultIndexFunctions.stream().anyMatch(func -> func.getName().equals(indexName))) {
            register.registerIndex(availableIndexFunctions.stream().filter(func -> func.apply(register).getName().equals(indexName)).findFirst().get().apply(register));
        }

        parseRsf(inputStream, register);

        if (indexRender.equals("indexTable")) {
            System.out.println(register.getIndex().toString());
        } else {
            if (indexRender.equals("current")) {
                printRecords(register.getCurrentIndex(indexName, indexValue, registerVersion), register);
            } else if (indexRender.equals("entries")) {
                printEntries(register.getRsfEntries(indexName, indexValue, registerVersion), register);
            }
        }
    }

    private static void parseRsf(InputStream inputStream, Register register) {
        RSFExecutor rsfExecutor = new RSFExecutor(register);
        rsfExecutor.register(new AddItemCommandHandler());
        rsfExecutor.register(new AppendEntryCommandHandler());

        RegisterSerialisationFormatService rsfService = new RegisterSerialisationFormatService(rsfExecutor);
        RegisterSerialisationFormat rsf = rsfService.readFrom(inputStream, new RSFFormatter());
        rsfService.process(rsf);
    }

    private static void printRecords(Map<String, List<HashValue>> records, Register register) {
        records.entrySet().stream().sorted((es1, es2) -> es1.getKey().compareTo(es2.getKey())).forEach(f -> {
            System.out.println(f.getKey() + ":");
            f.getValue().forEach(itemHash -> System.out.println("\t" + register.getItem(itemHash).getContent()));
        });
    }

    private static void printEntries(List<Entry> entries, Register register) {
        System.out.println(String.join("\n", entries.stream().flatMap(e -> e.getSha256hex().stream()).collect(Collectors.toSet())
                .stream().map(itemHash -> "add-item\t" + register.getItem(itemHash).getContent()).collect(Collectors.toList())));
        entries.stream().forEach(System.out::println);
    }

    public static void main(String[] args) {
//        File file = new File(args[0]);
        String indexName = args[0];
        String indexRender = args[1];
        Optional<Integer> registerVersion = args.length > 2 ? Optional.of(Integer.valueOf(args[2])) : Optional.empty();
        Optional<String> indexValue = args.length > 3 ? Optional.of(args[3]) : Optional.empty();

//        try {
            morc(System.in, indexName, indexRender, indexValue, registerVersion);
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
    }
}
