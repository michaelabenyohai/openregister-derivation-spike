package uk.gov.rsf.serialization;

import uk.gov.rsf.util.Record;

import javax.inject.Inject;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.List;

public class RegisterSerialisationFormatService {
    private final RSFExecutor rsfExecutor;

    @Inject
    public RegisterSerialisationFormatService(RSFExecutor rsfExecutor) {
        this.rsfExecutor = rsfExecutor;
    }

    public void process(RegisterSerialisationFormat rsf) {
        rsfExecutor.execute(rsf);
    }

    public RegisterSerialisationFormat readFrom(InputStream commandStream, RSFFormatter rsfFormatter) {
        BufferedReader buffer = new BufferedReader(new InputStreamReader(commandStream));
        Iterator<RegisterCommand> commandsIterator = buffer.lines()
                .map(rsfFormatter::parse)
                .iterator();
        return new RegisterSerialisationFormat(commandsIterator);
    }
}
