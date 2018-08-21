/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.nifi.processors.mongodb;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.JsonValidator;
import org.apache.nifi.processor.util.StandardValidators;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Tags({ "mongodb", "read", "get" })
@InputRequirement(Requirement.INPUT_ALLOWED)
@CapabilityDescription("Creates FlowFiles from documents in MongoDB loaded by a user-specified query.")
@WritesAttributes({
    @WritesAttribute(attribute = GetMongo.DB_NAME, description = "The database where the results came from."),
    @WritesAttribute(attribute = GetMongo.COL_NAME, description = "The collection where the results came from.")
})
public class GetMongo extends AbstractMongoProcessor {
    static final String DB_NAME = "mongo.database.name";
    static final String COL_NAME = "mongo.collection.name";

    static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("All FlowFiles that have the results of a successful query execution go here.")
            .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("All input FlowFiles that are part of a failed query execution go here.")
            .build();

    static final Relationship REL_ORIGINAL = new Relationship.Builder()
            .name("original")
            .description("All input FlowFiles that are part of a successful query execution go here.")
            .build();

    static final PropertyDescriptor QUERY = new PropertyDescriptor.Builder()
        .name("Query")
        .description("The selection criteria to do the lookup. If the field is left blank, it will look for input from" +
                " an incoming connection from another processor to provide the query as a valid JSON document inside of " +
                "the FlowFile's body. If this field is left blank and a timer is enabled instead of an incoming connection, " +
                "that will result in a full collection fetch using a \"{}\" query.")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .addValidator(JsonValidator.INSTANCE)
        .build();

    static final PropertyDescriptor PROJECTION = new PropertyDescriptor.Builder()
            .name("Projection")
            .description("The fields to be returned from the documents in the result set; must be a valid BSON document")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    static final PropertyDescriptor SORT = new PropertyDescriptor.Builder()
            .name("Sort")
            .description("The fields by which to sort; must be a valid BSON document")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(JsonValidator.INSTANCE)
            .build();

    static final PropertyDescriptor LIMIT = new PropertyDescriptor.Builder()
            .name("Limit")
            .description("The maximum number of elements to return")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final PropertyDescriptor BATCH_SIZE = new PropertyDescriptor.Builder()
            .name("Batch Size")
            .description("The number of elements to be returned from the server in one batch")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();
    static final PropertyDescriptor RESULTS_PER_FLOWFILE = new PropertyDescriptor.Builder()
            .name("results-per-flowfile")
            .displayName("Results Per FlowFile")
            .description("How many results to put into a FlowFile at once. The whole body will be treated as a JSON array of results.")
            .required(false)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
            .build();

    static final AllowableValue YES_PP = new AllowableValue("true", "True");
    static final AllowableValue NO_PP  = new AllowableValue("false", "False");

    static final PropertyDescriptor USE_PRETTY_PRINTING = new PropertyDescriptor.Builder()
            .name("use-pretty-printing")
            .displayName("Pretty Print Results JSON")
            .description("Choose whether or not to pretty print the JSON from the results of the query. " +
                    "Choosing 'True' can greatly increase the space requirements on disk depending on the complexity of the JSON document")
            .required(true)
            .defaultValue(YES_PP.getValue())
            .allowableValues(YES_PP, NO_PP)
            .addValidator(Validator.VALID)
            .build();

    private final static Set<Relationship> relationships;
    private final static List<PropertyDescriptor> propertyDescriptors;
    private ComponentLog logger;

    static {
        List<PropertyDescriptor> _propertyDescriptors = new ArrayList<>();
        _propertyDescriptors.addAll(descriptors);
        _propertyDescriptors.add(JSON_TYPE);
        _propertyDescriptors.add(USE_PRETTY_PRINTING);
        _propertyDescriptors.add(CHARSET);
        _propertyDescriptors.add(QUERY);
        _propertyDescriptors.add(QUERY_ATTRIBUTE);
        _propertyDescriptors.add(PROJECTION);
        _propertyDescriptors.add(SORT);
        _propertyDescriptors.add(LIMIT);
        _propertyDescriptors.add(BATCH_SIZE);
        _propertyDescriptors.add(RESULTS_PER_FLOWFILE);
        _propertyDescriptors.add(SSL_CONTEXT_SERVICE);
        _propertyDescriptors.add(CLIENT_AUTH);
        propertyDescriptors = Collections.unmodifiableList(_propertyDescriptors);

        final Set<Relationship> _relationships = new HashSet<>();
        _relationships.add(REL_SUCCESS);
        _relationships.add(REL_FAILURE);
        _relationships.add(REL_ORIGINAL);
        relationships = Collections.unmodifiableSet(_relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    //Turn a list of Mongo result documents into a String representation of a JSON array
    private String buildBatch(List<Document> documents, String jsonTypeSetting, String prettyPrintSetting) throws IOException {
        StringBuilder builder = new StringBuilder();
        for (int index = 0; index < documents.size(); index++) {
            Document document = documents.get(index);
            String asJson;
            if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                asJson = getObjectWriter(objectMapper, prettyPrintSetting).writeValueAsString(document);
            } else {
                asJson = document.toJson(new JsonWriterSettings(true));
            }
            builder
                    .append(asJson)
                    .append( (documents.size() > 1 && index + 1 < documents.size()) ? ", " : "" );
        }

        return "[" + builder.toString() + "]";
    }

    private ObjectWriter getObjectWriter(ObjectMapper mapper, String ppSetting) {
        return ppSetting.equals(YES_PP.getValue()) ? mapper.writerWithDefaultPrettyPrinter()
                : mapper.writer();
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile input = null;
        logger = getLogger();

        if (context.hasIncomingConnection()) {
            input = session.get();
            if (input == null && context.hasNonLoopConnection()) {
                return;
            }
        }

        final Document query = getQuery(context, session, input );
        if (query == null) {
            return;
        }

        final String jsonTypeSetting = context.getProperty(JSON_TYPE).getValue();
        final String usePrettyPrint  = context.getProperty(USE_PRETTY_PRINTING).getValue();
        final Charset charset = Charset.forName(context.getProperty(CHARSET).evaluateAttributeExpressions(input).getValue());


        final Map<String, String> attributes = new HashMap<>();

        attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");

        if (context.getProperty(QUERY_ATTRIBUTE).isSet()) {
            final String queryAttr = context.getProperty(QUERY_ATTRIBUTE).evaluateAttributeExpressions(input).getValue();
            attributes.put(queryAttr, query.toJson());
        }

        final Document projection = context.getProperty(PROJECTION).isSet()
                ? Document.parse(context.getProperty(PROJECTION).evaluateAttributeExpressions(input).getValue()) : null;
        final Document sort = context.getProperty(SORT).isSet()
                ? Document.parse(context.getProperty(SORT).evaluateAttributeExpressions(input).getValue()) : null;

        final MongoCollection<Document> collection = getCollection(context, input);
        final FindIterable<Document> it = collection.find(query);

        attributes.put(DB_NAME, collection.getNamespace().getDatabaseName());
        attributes.put(COL_NAME, collection.getNamespace().getCollectionName());

        if (projection != null) {
            it.projection(projection);
        }
        if (sort != null) {
            it.sort(sort);
        }
        if (context.getProperty(LIMIT).isSet()) {
            it.limit(context.getProperty(LIMIT).evaluateAttributeExpressions(input).asInteger());
        }
        if (context.getProperty(BATCH_SIZE).isSet()) {
            it.batchSize(context.getProperty(BATCH_SIZE).evaluateAttributeExpressions(input).asInteger());
        }

        try (MongoCursor<Document> cursor = it.iterator()) {
            final List<Document> listOfDocuments = new ArrayList<>();
            final FlowFile inputFF = input;
            Document doc;

            configureMapper(jsonTypeSetting);

            while ((doc = cursor.tryNext()) != null) {
                listOfDocuments.add(doc);
            }

            if (context.getProperty(RESULTS_PER_FLOWFILE).isSet()) {
                final int sizePerChunk = context.getProperty(RESULTS_PER_FLOWFILE).evaluateAttributeExpressions(input).asInteger();
                List<List<Document>> chunks = chunkDocumentsList(listOfDocuments, sizePerChunk);

                chunks.parallelStream().forEach( documentList -> {
                    try {
                        String payload = buildBatch(documentList, jsonTypeSetting, usePrettyPrint);
                        writeBatch(payload, inputFF, context, session, attributes, REL_SUCCESS);
                    } catch (Exception e) {
                        logger.error("Error building batch due to {}", new Object[] {e});
                    }
                });
            } else {
                listOfDocuments.parallelStream().forEach( document -> {
                    FlowFile outgoingFlowFile = (inputFF == null) ? session.create() : session.create(inputFF);

                    outgoingFlowFile = session.write(outgoingFlowFile, out -> {
                        String json;

                        if (jsonTypeSetting.equals(JSON_TYPE_STANDARD)) {
                            json = getObjectWriter(objectMapper, usePrettyPrint).writeValueAsString(document);
                        } else {
                            json = document.toJson();
                        }
                        out.write(json.getBytes(charset));
                    });

                    outgoingFlowFile = session.putAllAttributes(outgoingFlowFile, attributes);

                    session.getProvenanceReporter().receive(outgoingFlowFile, getURI(context));
                    session.transfer(outgoingFlowFile, REL_SUCCESS);
                });
            }

            if (input != null) {
                session.transfer(input, REL_ORIGINAL);
            }
        }

    }

    private Document getQuery(ProcessContext context, ProcessSession session, FlowFile input) {
        Document query = null;
        if (context.getProperty(QUERY).isSet()) {
            query = Document.parse(context.getProperty(QUERY).evaluateAttributeExpressions(input).getValue());
        } else if (!context.getProperty(QUERY).isSet() && input == null) {
            query = Document.parse("{}");
        } else {
            try {
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                session.exportTo(input, out);
                out.close();
                query = Document.parse(new String(out.toByteArray()));
            } catch (Exception ex) {
                logger.error("Error reading FlowFile : ", ex);
                if (input != null) { //Likely culprit is a bad query
                    session.transfer(input, REL_FAILURE);
                    session.commit();
                } else {
                    throw new ProcessException(ex);
                }
            }
        }

        return query;
    }

    private List<List<Document>> chunkDocumentsList(List<Document> originalList, int perChunkSize) {
        final List<List<Document>> chunks = new ArrayList<>();
        final int originalSize = originalList.size();

        for (int i = 0; i < originalSize; i += perChunkSize) {
            chunks.add(new ArrayList<>(
                    originalList.subList(i, Math.min(originalSize, i + perChunkSize)))
            );
        }

        return chunks;
    }
}
