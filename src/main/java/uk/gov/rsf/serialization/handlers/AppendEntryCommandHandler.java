package uk.gov.rsf.serialization.handlers;

import uk.gov.rsf.util.Entry;
import uk.gov.rsf.util.HashingAlgorithm;
import uk.gov.rsf.util.Register;
import uk.gov.rsf.serialization.RegisterCommand;
import uk.gov.rsf.serialization.RegisterCommandHandler;
import uk.gov.rsf.serialization.RegisterResult;
import uk.gov.rsf.util.HashValue;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class AppendEntryCommandHandler extends RegisterCommandHandler {
    @Override
    protected RegisterResult executeCommand(RegisterCommand command, Register register) {
//        try {
            List<String> parts = command.getCommandArguments();
            int newEntryNo = register.getLatestEntryNumber() + 1;
            String hashes = parts.get(1).replace("[", "").replace("]", "");
            List<HashValue> hashList = Arrays.asList(hashes.split(",")).stream().map(h -> HashValue.decode(HashingAlgorithm.SHA256, h)).collect(Collectors.toList());
            Entry entry = new Entry(newEntryNo, hashList, Instant.parse(parts.get(0)), parts.get(2));
            register.appendEntry(entry);
            return RegisterResult.createSuccessResult();

        //Arrays.asList(HashValue.decode(HashingAlgorithm.SHA256, parts.get(1)))
//        } catch (Exception e) {
//            return RegisterResult.createFailResult("Exception when executing command: " + command, e);
//        }
    }

    @Override
    public String getCommandName() {
        return "append-entry";
    }
}
