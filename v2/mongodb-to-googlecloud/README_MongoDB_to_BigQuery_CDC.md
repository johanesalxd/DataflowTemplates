
MongoDB (CDC) to BigQuery template
---
The MongoDB CDC (Change Data Capture) to BigQuery template is a streaming
pipeline that works together with MongoDB change streams. The pipeline reads the
JSON records pushed to Pub/Sub or Kafka via a MongoDB change stream and writes them to
BigQuery as specified by the <code>userOption</code> parameter.


:memo: This is a Google-provided template! Please
check [Provided templates documentation](https://cloud.google.com/dataflow/docs/guides/templates/provided/mongodb-change-stream-to-bigquery)
on how to use it without having to build from sources using [Create job from template](https://console.cloud.google.com/dataflow/createjob?template=MongoDB_to_BigQuery_CDC).

:bulb: This is a generated documentation based
on [Metadata Annotations](https://github.com/GoogleCloudPlatform/DataflowTemplates#metadata-annotations)
. Do not change this file directly.

## Parameters

### Required parameters

* **mongoDbUri**: The MongoDB connection URI in the format `mongodb+srv://:@.`.
* **database**: Database in MongoDB to read the collection from. For example, `my-db`.
* **collection**: Name of the collection inside MongoDB database. For example, `my-collection`.
* **userOption**: `FLATTEN`, `JSON`, or `NONE`. `FLATTEN` flattens the documents to the single level. `JSON` stores document in BigQuery JSON format. `NONE` stores the whole document as a JSON-formatted STRING. Defaults to: NONE.
* **inputTopic**: The Pub/Sub input topic to read from, in the format of `projects/<PROJECT_ID>/topics/<TOPIC_NAME>`. Required if Kafka is not used as the source.
* **outputTableSpec**: The BigQuery table to write to. For example, `bigquery-project:dataset.output_table`.

### Optional parameters

* **kafkaBootstrapServers**: Kafka Bootstrap Server(s) list to connect to for CDC events. Required if Kafka is used as the source. For example, `broker_1:9092,broker_2:9092`.
* **kafkaReadTopics**: Kafka topic(s) to read CDC input from. Required if Kafka is used as the source. For example, `topic1,topic2`.
* **kafkaConsumerConfig**: Other Kafka consumer configurations, as a comma-separated list of key=value pairs. See Apache Kafka documentation for a list of available options. For example, `group.id=mygroup,auto.offset.reset=earliest`.
* **kafkaSaslJaasConfigSecret**: Secret Manager ID for Kafka SASL/JAAS config, if using SASL for authentication (e.g., projects/my-project/secrets/my-secret/versions/latest). Required if SASL is used and `kafkaSaslMechanism` is set.
* **kafkaSaslMechanism**: SASL mechanism for Kafka authentication (e.g., PLAIN, SCRAM-SHA-256, SCRAM-SHA-512, GSSAPI). Used in conjunction with `kafkaSaslJaasConfigSecret`.
* **useStorageWriteApiAtLeastOnce**: When using the Storage Write API, specifies the write semantics. To use at-least-once semantics (https://beam.apache.org/documentation/io/built-in/google-bigquery/#at-least-once-semantics), set this parameter to `true`. To use exactly- once semantics, set the parameter to `false`. This parameter applies only when `useStorageWriteApi` is `true`. The default value is `false`.
* **KMSEncryptionKey**: Cloud KMS Encryption Key to decrypt the mongodb uri connection string or the Kafka SASL/JAAS configuration. If Cloud KMS key is passed in, the mongodb uri connection string or Kafka SASL/JAAS configuration string must all be passed in encrypted. For example, `projects/your-project/locations/global/keyRings/your-keyring/cryptoKeys/your-key`.
* **filter**: Bson filter in json format. For example, `{ "val": { $gt: 0, $lt: 9 }}`.
* **useStorageWriteApi**: If true, the pipeline uses the BigQuery Storage Write API (https://cloud.google.com/bigquery/docs/write-api). The default value is `false`. For more information, see Using the Storage Write API (https://beam.apache.org/documentation/io/built-in/google-bigquery/#storage-write-api).
* **numStorageWriteApiStreams**: When using the Storage Write API, specifies the number of write streams. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter. Defaults to: 0.
* **storageWriteApiTriggeringFrequencySec**: When using the Storage Write API, specifies the triggering frequency, in seconds. If `useStorageWriteApi` is `true` and `useStorageWriteApiAtLeastOnce` is `false`, then you must set this parameter.
* **bigQuerySchemaPath**: The Cloud Storage path for the BigQuery JSON schema. For example, `gs://your-bucket/your-schema.json`.
* **javascriptDocumentTransformGcsPath**: The Cloud Storage URI of the `.js` file that defines the JavaScript user-defined function (UDF) to use. For example, `gs://your-bucket/your-transforms/*.js`.
* **javascriptDocumentTransformFunctionName**: The name of the JavaScript user-defined function (UDF) to use. For example, if your JavaScript function code is `myTransform(inJson) { /*...do stuff...*/ }`, then the function name is myTransform. For sample JavaScript UDFs, see UDF Examples (https://github.com/GoogleCloudPlatform/DataflowTemplates#udf-examples). For example, `transform`.



## Getting Started

### Requirements

* Java 17
* Maven
* [gcloud CLI](https://cloud.google.com/sdk/gcloud), and execution of the
  following commands:
  * `gcloud auth login`
  * `gcloud auth application-default login`

:star2: Those dependencies are pre-installed if you use Google Cloud Shell!

[![Open in Cloud Shell](http://gstatic.com/cloudssh/images/open-btn.svg)](https://console.cloud.google.com/cloudshell/editor?cloudshell_git_repo=https%3A%2F%2Fgithub.com%2FGoogleCloudPlatform%2FDataflowTemplates.git&cloudshell_open_in_editor=v2/mongodb-to-googlecloud/src/main/java/com/google/cloud/teleport/v2/mongodb/templates/MongoDbCdcToBigQuery.java)

### Templates Plugin

This README provides instructions using
the [Templates Plugin](https://github.com/GoogleCloudPlatform/DataflowTemplates#templates-plugin).

### Building Template

This template is a Flex Template, meaning that the pipeline code will be
containerized and the container will be executed on Dataflow. Please
check [Use Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/using-flex-templates)
and [Configure Flex Templates](https://cloud.google.com/dataflow/docs/guides/templates/configuring-flex-templates)
for more information.

#### Staging the Template

If the plan is to just stage the template (i.e., make it available to use) by
the `gcloud` command or Dataflow "Create job from template" UI,
the `-PtemplatesStage` profile should be used:

```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>

mvn clean package -PtemplatesStage  \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-DstagePrefix="templates" \
-DtemplateName="MongoDB_to_BigQuery_CDC" \
-f v2/mongodb-to-googlecloud
```


The command should build and save the template to Google Cloud, and then print
the complete location on Cloud Storage:

```
Flex Template was staged! gs://<bucket-name>/templates/flex/MongoDB_to_BigQuery_CDC
```

The specific path should be copied as it will be used in the following steps.

#### Running the Template

**Using the staged template**:

You can use the path above run the template (or share with others for execution).

To start a job with the template at any time using `gcloud`, you are going to
need valid resources for the required parameters.

Provided that, the following command line can be used (choose one source: Pub/Sub or Kafka):

**Pub/Sub Source:**
```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/MongoDB_to_BigQuery_CDC"

### Required
export MONGO_DB_URI=<mongoDbUri>
export DATABASE=<database>
export COLLECTION=<collection>
export USER_OPTION=NONE
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export FILTER=<filter>
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export BIG_QUERY_SCHEMA_PATH=<bigQuerySchemaPath>
export JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH=<javascriptDocumentTransformGcsPath>
export JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME=<javascriptDocumentTransformFunctionName>

gcloud dataflow flex-template run "mongodb-to-bigquery-cdc-pubsub-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE" \
  --parameters "mongoDbUri=$MONGO_DB_URI" \
  --parameters "database=$DATABASE" \
  --parameters "collection=$COLLECTION" \
  --parameters "userOption=$USER_OPTION" \
  --parameters "KMSEncryptionKey=$KMSENCRYPTION_KEY" \
  --parameters "filter=$FILTER" \
  --parameters "useStorageWriteApi=$USE_STORAGE_WRITE_API" \
  --parameters "numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS" \
  --parameters "storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC" \
  --parameters "inputTopic=$INPUT_TOPIC" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH" \
  --parameters "javascriptDocumentTransformGcsPath=$JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH" \
  --parameters "javascriptDocumentTransformFunctionName=$JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME"
```

**Kafka Source:**
```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1
export TEMPLATE_SPEC_GCSPATH="gs://$BUCKET_NAME/templates/flex/MongoDB_to_BigQuery_CDC"

### Required
export MONGO_DB_URI=<mongoDbUri>
export DATABASE=<database>
export COLLECTION=<collection>
export OUTPUT_TABLE_SPEC=<outputTableSpec>
export KAFKA_BOOTSTRAP_SERVERS=<kafkaBootstrapServers> ## Kafka Bootstrap Servers
export KAFKA_READ_TOPICS=<kafkaReadTopics> ## Kafka Topics

### Optional
export USER_OPTION=NONE
export KAFKA_CONSUMER_CONFIG=<kafkaConsumerConfig>
export KAFKA_SASL_JAAS_CONFIG_SECRET=<kafkaSaslJaasConfigSecret>
export KAFKA_SASL_MECHANISM=<kafkaSaslMechanism>
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
# ... other optional parameters ...

gcloud dataflow flex-template run "mongodb-to-bigquery-cdc-kafka-job" \
  --project "$PROJECT" \
  --region "$REGION" \
  --template-file-gcs-location "$TEMPLATE_SPEC_GCSPATH" \
  --parameters "mongoDbUri=$MONGO_DB_URI" \
  --parameters "database=$DATABASE" \
  --parameters "collection=$COLLECTION" \
  --parameters "outputTableSpec=$OUTPUT_TABLE_SPEC" \
  --parameters "kafkaBootstrapServers=$KAFKA_BOOTSTRAP_SERVERS" \
  --parameters "kafkaReadTopics=$KAFKA_READ_TOPICS" \
  --parameters "userOption=$USER_OPTION" \
  --parameters "kafkaConsumerConfig=$KAFKA_CONSUMER_CONFIG" \
  --parameters "kafkaSaslJaasConfigSecret=$KAFKA_SASL_JAAS_CONFIG_SECRET" \
  --parameters "kafkaSaslMechanism=$KAFKA_SASL_MECHANISM" \
  --parameters "KMSEncryptionKey=$KMSENCRYPTION_KEY"
  # ... other optional parameters ...
```

For more information about the command, please check:
https://cloud.google.com/sdk/gcloud/reference/dataflow/flex-template/run


**Using the plugin**:

Instead of just generating the template in the folder, it is possible to stage
and run the template in a single command. This may be useful for testing when
changing the templates.

**(Example for Pub/Sub source, adapt for Kafka accordingly)**
```shell
export PROJECT=<my-project>
export BUCKET_NAME=<bucket-name>
export REGION=us-central1

### Required
export MONGO_DB_URI=<mongoDbUri>
export DATABASE=<database>
export COLLECTION=<collection>
export USER_OPTION=NONE
export INPUT_TOPIC=<inputTopic>
export OUTPUT_TABLE_SPEC=<outputTableSpec>

### Optional
export USE_STORAGE_WRITE_API_AT_LEAST_ONCE=false
export KMSENCRYPTION_KEY=<KMSEncryptionKey>
export FILTER=<filter>
export USE_STORAGE_WRITE_API=false
export NUM_STORAGE_WRITE_API_STREAMS=0
export STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC=<storageWriteApiTriggeringFrequencySec>
export BIG_QUERY_SCHEMA_PATH=<bigQuerySchemaPath>
export JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH=<javascriptDocumentTransformGcsPath>
export JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME=<javascriptDocumentTransformFunctionName>

mvn clean package -PtemplatesRun \
-DskipTests \
-DprojectId="$PROJECT" \
-DbucketName="$BUCKET_NAME" \
-Dregion="$REGION" \
-DjobName="mongodb-to-bigquery-cdc-job" \
-DtemplateName="MongoDB_to_BigQuery_CDC" \
-Dparameters="useStorageWriteApiAtLeastOnce=$USE_STORAGE_WRITE_API_AT_LEAST_ONCE,mongoDbUri=$MONGO_DB_URI,database=$DATABASE,collection=$COLLECTION,userOption=$USER_OPTION,KMSEncryptionKey=$KMSENCRYPTION_KEY,filter=$FILTER,useStorageWriteApi=$USE_STORAGE_WRITE_API,numStorageWriteApiStreams=$NUM_STORAGE_WRITE_API_STREAMS,storageWriteApiTriggeringFrequencySec=$STORAGE_WRITE_API_TRIGGERING_FREQUENCY_SEC,inputTopic=$INPUT_TOPIC,outputTableSpec=$OUTPUT_TABLE_SPEC,bigQuerySchemaPath=$BIG_QUERY_SCHEMA_PATH,javascriptDocumentTransformGcsPath=$JAVASCRIPT_DOCUMENT_TRANSFORM_GCS_PATH,javascriptDocumentTransformFunctionName=$JAVASCRIPT_DOCUMENT_TRANSFORM_FUNCTION_NAME" \
-f v2/mongodb-to-googlecloud
```

## Terraform

Dataflow supports the utilization of Terraform to manage template jobs,
see [dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job).

Terraform modules have been generated for most templates in this repository. This includes the relevant parameters
specific to the template. If available, they may be used instead of
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly.

To use the autogenerated module, execute the standard
[terraform workflow](https://developer.hashicorp.com/terraform/intro/core-workflow):

```shell
cd v2/mongodb-to-googlecloud/terraform/MongoDB_to_BigQuery_CDC
terraform init
terraform apply
```

To use
[dataflow_flex_template_job](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataflow_flex_template_job)
directly (choose one source: Pub/Sub or Kafka):

**Kafka Source:**
```terraform
provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

resource "google_dataflow_flex_template_job" "mongodb_to_bigquery_cdc_kafka" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/MongoDB_to_BigQuery_CDC"
  name              = "mongodb-to-bigquery-cdc-kafka"
  region            = var.region
  parameters        = {
    mongoDbUri = "<mongoDbUri>"
    database = "<database>"
    collection = "<collection>"
    outputTableSpec = "<outputTableSpec>"
    kafkaBootstrapServers = "<kafkaBootstrapServers>" // Kafka Bootstrap Servers
    kafkaReadTopics = "<kafkaReadTopics>" // Kafka Topics
    // userOption = "NONE"
    // kafkaConsumerConfig = "<kafkaConsumerConfig>"
    // kafkaSaslJaasConfigSecret = "<kafkaSaslJaasConfigSecret>"
    // kafkaSaslMechanism = "<kafkaSaslMechanism>"
    // KMSEncryptionKey = "<KMSEncryptionKey>"
    // ... other optional parameters
  }
}
```

**Pub/Sub Source:**
```terraform
provider "google-beta" {
  project = var.project
}
variable "project" {
  default = "<my-project>"
}
variable "region" {
  default = "us-central1"
}

resource "google_dataflow_flex_template_job" "mongodb_to_bigquery_cdc_pubsub" {

  provider          = google-beta
  container_spec_gcs_path = "gs://dataflow-templates-${var.region}/latest/flex/MongoDB_to_BigQuery_CDC"
  name              = "mongodb-to-bigquery-cdc-pubsub"
  region            = var.region
  parameters        = {
    mongoDbUri = "<mongoDbUri>"
    database = "<database>"
    collection = "<collection>"
    userOption = "NONE"
    inputTopic = "<inputTopic>"
    outputTableSpec = "<outputTableSpec>"
    # useStorageWriteApiAtLeastOnce = "false"
    # KMSEncryptionKey = "<KMSEncryptionKey>"
    # filter = "<filter>"
    # useStorageWriteApi = "false"
    # numStorageWriteApiStreams = "0"
    # storageWriteApiTriggeringFrequencySec = "<storageWriteApiTriggeringFrequencySec>"
    # bigQuerySchemaPath = "<bigQuerySchemaPath>"
    # javascriptDocumentTransformGcsPath = "<javascriptDocumentTransformGcsPath>"
    # javascriptDocumentTransformFunctionName = "<javascriptDocumentTransformFunctionName>"
  }
}
```
