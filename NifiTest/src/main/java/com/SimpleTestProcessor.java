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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.ByteArrayInputStream;
import org.w3c.dom.Document;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@SideEffectFree
@Tags({"JSON", "NIFI ROCKS"})

@CapabilityDescription("Fetch value from json path.")
public class SimpleTestProcessor extends AbstractProcessor{


    private List<PropertyDescriptor> properties;
    private Set<Relationship> relationships;
    private int testVariable = 0;

    public static final String MATCH_ATTR = "match";

/*    public static final PropertyDescriptor JSON_PATH = new PropertyDescriptor.Builder()
            .name("Json Path")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();*/

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Success relationship")
            .build();

    public static final Relationship ERROR = new Relationship.Builder()
            .name("Error")
            .description("Error relationship")
            .build();


    public SimpleTestProcessor() {
    }

    @Override
    public void onTrigger(ProcessContext processContext, final ProcessSession processSession) throws ProcessException {

        testVariable++;

        final ProcessorLog log = this.getLogger();
        final AtomicReference<String> value = new AtomicReference<>();

        FlowFile flowfile = processSession.get();

        log.warn(String.valueOf(testVariable));
        //log.warn(String.valueOf(new Date(flowfile.getEntryDate())));

        //value.set("Hej");

        processSession.read(flowfile, new InputStreamCallback() {
            @Override
            public void process(InputStream inputStream) throws IOException {
                String xml =IOUtils.toString(inputStream);

                DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
                DocumentBuilder builder;
                try {
                    builder = factory.newDocumentBuilder();
                    Document document = builder.parse(new InputSource(new ByteArrayInputStream(xml.getBytes("utf-8"))));
                    value.set(Integer.toString(document.getElementsByTagName("se-gen-base:Soliditet").getLength()));


                    //value.set("Hej");
                } catch (Exception e) {
                    e.printStackTrace();

                }
            }
        });

        String results = value.get();
        if(results != null && !results.isEmpty()){
            flowfile = processSession.putAttribute(flowfile, "match", results);
        }

        // To write the results back out ot flow file
        flowfile = processSession.write(flowfile, new OutputStreamCallback() {

            @Override
            public void process(OutputStream out) throws IOException {
                out.write(value.get().getBytes());
            }
        });

        processSession.transfer(flowfile, SUCCESS);

    }

    @Override
    public void init(final ProcessorInitializationContext context){

        //this.properties = Collections.unmodifiableList(properties);

        Set<Relationship> relationships = new HashSet<>();
        relationships.add(SUCCESS);
        relationships.add(ERROR);
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
