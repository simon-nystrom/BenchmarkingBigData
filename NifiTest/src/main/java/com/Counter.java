package com;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@Tags({"Count"})

@CapabilityDescription("Fetch value from json path.")
public class Counter extends AbstractProcessor {


    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private int counter = 0;

    public static final String MATCH_ATTR = "match";

/*    public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
            .name("Json Path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();*/

    public static final Relationship REL_DONE = new Relationship.Builder()
            .name("done")
            .description("emits the flowfile when the counter reaches 10000")
            .build();

    public static final Relationship REL_DUMP = new Relationship.Builder()
            .name("dump")
            .description("relationship for auto-termination to dump flowfiles")
            .build();



    public Counter() {
    }

    @Override
    public void onTrigger(ProcessContext processContext, final ProcessSession processSession) throws ProcessException {

        final ProcessorLog log = this.getLogger();
        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = processSession.get();

        counter++;

        if(counter == 10000) {
            processSession.transfer(flowfile, REL_DONE);
        } else {
            processSession.transfer(flowfile, REL_DUMP);
        }


    }

    @Override
    public void init(final ProcessorInitializationContext context){

        //this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_DONE);
        relationships.add(REL_DUMP);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return properties;
    }


}
