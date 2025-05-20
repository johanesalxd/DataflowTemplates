/*
 * Copyright (C) 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.teleport.v2.mongodb.templates;

import static com.google.cloud.teleport.v2.utils.KMSUtils.maybeDecrypt;

import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.teleport.metadata.Template;
import com.google.cloud.teleport.metadata.TemplateCategory;
import com.google.cloud.teleport.metadata.TemplateParameter;
import com.google.cloud.teleport.v2.common.UncaughtExceptionLogger;
import com.google.cloud.teleport.v2.kafka.utils.KafkaCommonUtils;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.BigQueryWriteOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.JavascriptDocumentTransformerOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.KafkaOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.MongoDbOptions;
import com.google.cloud.teleport.v2.mongodb.options.MongoDbToBigQueryOptions.PubSubOptions;
import com.google.cloud.teleport.v2.mongodb.templates.MongoDbCdcToBigQuery.Options;
import com.google.cloud.teleport.v2.options.BigQueryStorageApiStreamingOptions;
import com.google.cloud.teleport.v2.transforms.JavascriptDocumentTransformer.TransformDocumentViaJavascript;
import com.google.cloud.teleport.v2.utils.BigQueryIOUtils;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.script.ScriptException;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@link MongoDbCdcToBigQuery} pipeline is a streaming pipeline which reads data pushed to
 * PubSub or Kafka from MongoDB Changestream and outputs the resulting records to BigQuery.
 *
 * <p>Check out <a
 * href="https://github.com/GoogleCloudPlatform/DataflowTemplates/blob/main/v2/mongodb-to-googlecloud/README_MongoDB_to_BigQuery_CDC.md">README</a>
 * for instructions on how to use or modify this template.
 */
@Template(
    name = "MongoDB_to_BigQuery_CDC",
    category = TemplateCategory.STREAMING,
    displayName = "MongoDB (CDC) to BigQuery (via Pub/Sub or Kafka)",
    description =
        "The MongoDB CDC (Change Data Capture) to BigQuery template is a streaming pipeline that works together with MongoDB change streams. "
            + "The pipeline reads the JSON records pushed to Pub/Sub topic or Kafka topic(s) via a MongoDB change stream and writes them to BigQuery as specified by the <code>userOption</code> parameter.",
    optionsClass = Options.class,
    flexContainerName = "mongodb-to-bigquery-cdc",
    documentation =
        "https://cloud.google.com/dataflow/docs/guides/templates/provided/mongodb-change-stream-to-bigquery",
    contactInformation = "https://cloud.google.com/support",
    preview = true,
    requirements = {
      "The target BigQuery dataset must exist.",
      "The source MongoDB instance must be accessible from the Dataflow worker machines.",
      "The change stream pushing changes from MongoDB to the source (Pub/Sub or Kafka) should be running.",
      "If using Kafka as a source, the Kafka cluster must be accessible from the Dataflow worker machines."
    },
    streaming = true,
    supportsAtLeastOnce = true)
public class MongoDbCdcToBigQuery {

  private static final Logger LOG = LoggerFactory.getLogger(MongoDbCdcToBigQuery.class);

  /** Options interface. */
  public interface Options
      extends PipelineOptions,
          MongoDbOptions,
          PubSubOptions,
          KafkaOptions,
          BigQueryWriteOptions,
          JavascriptDocumentTransformerOptions,
          BigQueryStorageApiStreamingOptions {

    // Hide the UseStorageWriteApiAtLeastOnce in the UI, because it will automatically be turned
    // on when pipeline is running on ALO mode and using the Storage Write API
    @TemplateParameter.Boolean(
        order = 1,
        optional = true,
        parentName = "useStorageWriteApi",
        parentTriggerValues = {"true"},
        description = "Use at at-least-once semantics in BigQuery Storage Write API",
        helpText =
            "When using the Storage Write API, specifies the write semantics. To"
                + " use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly-"
                + " once semantics, set the parameter to `false`. This parameter applies only when"
                + " `useStorageWriteApi` is `true`. The default value is `false`.",
        hiddenUi = true)
    @Default.Boolean(false)
    @Override
    Boolean getUseStorageWriteApiAtLeastOnce();

    void setUseStorageWriteApiAtLeastOnce(Boolean value);
  }

  /** class ParseAsDocumentsFn. */
  private static class ParseAsDocumentsFn extends DoFn<String, Document> {

    @ProcessElement
    public void processElement(ProcessContext context) {
      context.output(Document.parse(context.element()));
    }
  }

  /**
   * Main entry point for pipeline execution.
   *
   * @param args Command line arguments to the pipeline.
   */
  public static void main(String[] args)
      throws ScriptException, IOException, NoSuchMethodException {
    UncaughtExceptionLogger.register();

    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    BigQueryIOUtils.validateBQStorageApiOptionsStreaming(options);
    run(options);
  }

  /** Pipeline to read data from source and write to MongoDB. */
  public static boolean run(Options options)
      throws ScriptException, IOException, NoSuchMethodException {
    options.setStreaming(true);
    Pipeline pipeline = Pipeline.create(options);
    String userOption = options.getUserOption();
    String inputOption = options.getInputTopic();

    // Validate that either Pub/Sub or Kafka options are provided, but not both.
    boolean pubsubProvided = options.getInputTopic() != null && !options.getInputTopic().isEmpty();
    boolean kafkaProvided =
        options.getKafkaBootstrapServers() != null
            && !options.getKafkaBootstrapServers().isEmpty()
            && options.getKafkaReadTopics() != null
            && !options.getKafkaReadTopics().isEmpty();

    if (pubsubProvided == kafkaProvided) { // either both are provided or none are
      throw new IllegalArgumentException(
          "Either Pub/Sub topic or Kafka bootstrap servers and topics must be provided, but not both.");
    }

    TableSchema bigquerySchema;

    // Get MongoDbUri
    String mongoDbUri = maybeDecrypt(options.getMongoDbUri(), options.getKMSEncryptionKey()).get();

    if (options.getJavascriptDocumentTransformFunctionName() != null
        && options.getJavascriptDocumentTransformGcsPath() != null) {
      bigquerySchema =
          MongoDbUtils.getTableFieldSchemaForUDF(
              mongoDbUri,
              options.getDatabase(),
              options.getCollection(),
              options.getJavascriptDocumentTransformGcsPath(),
              options.getJavascriptDocumentTransformFunctionName(),
              options.getUserOption());
    } else {
      bigquerySchema =
          MongoDbUtils.getTableFieldSchema(
              mongoDbUri, options.getDatabase(), options.getCollection(), options.getUserOption());
    }

    PCollection<String> messages;
    if (pubsubProvided) {
      messages =
          pipeline
            .apply("Read PubSub Messages", PubsubIO.readStrings().fromTopic(inputOption));
    } else {
      Map<String, Object> consumerConfig = new HashMap<>();
      if (options.getKafkaConsumerConfig() != null && !options.getKafkaConsumerConfig().isEmpty()) {
        consumerConfig = KafkaCommonUtils.parseKafkaClientConfig(options.getKafkaConsumerConfig());
      }
      if (options.getKafkaSaslJaasConfigSecret() != null
          && !options.getKafkaSaslJaasConfigSecret().isEmpty()) {
        consumerConfig.put(
            "sasl.jaas.config",
            maybeDecrypt(options.getKafkaSaslJaasConfigSecret(), options.getKMSEncryptionKey())
                .get());
      }
      if (options.getKafkaSaslMechanism() != null && !options.getKafkaSaslMechanism().isEmpty()) {
        consumerConfig.put("sasl.mechanism", options.getKafkaSaslMechanism());
      }

      messages =
          pipeline.apply(
              "Read Kafka Messages",
              KafkaIO.<String, String>read()
                  .withBootstrapServers(options.getKafkaBootstrapServers())
                  .withTopics(options.getKafkaReadTopics())
                  .withKeyDeserializer(StringDeserializer.class) // Key is ignored for now
                  .withValueDeserializer(StringDeserializer.class)
                  .withConsumerConfigUpdates(consumerConfig)
                  .withoutMetadata() // We only need the message value (JSON string)
                  .apply(
                      "Extract Kafka Message Value",
                      MapElements.into(TypeDescriptors.strings()).via(record -> record.getKV().getValue())));
    }

    messages
        .apply(
            "RTransform string to document",
            ParDo.of(
                new DoFn<String, Document>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = Document.parse(c.element());
                    c.output(document);
                  }
                }))
        .apply(
            "UDF",
            TransformDocumentViaJavascript.newBuilder()
                .setFileSystemPath(options.getJavascriptDocumentTransformGcsPath())
                .setFunctionName(options.getJavascriptDocumentTransformFunctionName())
                .build())
        .apply(
            "Read and transform data",
            ParDo.of(
                new DoFn<Document, TableRow>() {
                  @ProcessElement
                  public void process(ProcessContext c) {
                    Document document = c.element();
                    TableRow row = MongoDbUtils.getTableSchema(document, userOption);
                    c.output(row);
                  }
                }))
        .apply(
            BigQueryIO.writeTableRows()
                .to(options.getOutputTableSpec())
                .withSchema(bigquerySchema)
                .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));
    pipeline.run();
    return true;
  }
}
