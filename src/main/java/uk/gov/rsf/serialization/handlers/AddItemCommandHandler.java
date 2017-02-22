package uk.gov.rsf.serialization.handlers;

import uk.gov.rsf.util.Item;
import uk.gov.rsf.serialization.RegisterCommand;
import uk.gov.rsf.serialization.RegisterCommandHandler;
import uk.gov.rsf.serialization.RegisterResult;
import uk.gov.rsf.util.ObjectReconstructor;
import uk.gov.rsf.util.Register;

public class AddItemCommandHandler extends RegisterCommandHandler {
    private final ObjectReconstructor objectReconstructor;

    public AddItemCommandHandler() {
        objectReconstructor = new ObjectReconstructor();
    }

    @Override
    protected RegisterResult executeCommand(RegisterCommand command, Register register) {
        try {
            String jsonContent = command.getCommandArguments().get(0);
            Item item = new Item(objectReconstructor.reconstruct(jsonContent));
            register.putItem(item);
            return RegisterResult.createSuccessResult();
        } catch (Exception e) {
            return RegisterResult.createFailResult("Exception when executing command: " + command, e);
        }
    }

    @Override
    public String getCommandName() {
        return "add-item";
    }
}
