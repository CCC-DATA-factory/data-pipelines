import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

// Attributes to inherit
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Get database_name and collection_name from flowFile attributes
def database_name = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collection_name = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

// Read FlowFile content into JSON string
String inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON
def parser = new JsonSlurper()
def records
try {
    records = parser.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Input is not a list of JSON objects")
        session.transfer(session.putAttribute(flowFile, "error", "Expected a list of JSON objects"), REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    session.transfer(session.putAttribute(flowFile, "error", "Invalid JSON format: ${e.message}"), REL_FAILURE)
    return
}

// Current timestamp in Tunis timezone
long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Prepare output and failure lists
List outputRecords = []
List failureRecords = []

// Iterate and validate/transform records
records.eachWithIndex { record, idx ->
    def errors = []

    // Transform
    def transformed = [
        id                  : record._id ?: null,
        id_user             : record.user ?: null,
        role                : "admin",
        parent_id           : null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null
    ]

    // Validate
    if (!(record._id instanceof String)) errors << "_id must be a string"
    if (!(record.user instanceof String)) errors << "user must be a string"

    def outRec = new LinkedHashMap<>(transformed)
    outRec['is_valid'] = errors.isEmpty()
    outRec['comment'] = errors.isEmpty() ? null : errors.join('; ')

    outputRecords << outRec

    // Prepare failure record if needed
    if (!errors.isEmpty()) {
        failureRecords << [
            database_name   : database_name,
            collection_name : collection_name,
            record_id       : record._id ?: null,
            error_message   : errors.join('; ')
        ]
    }
}

// Write output records to new FlowFile
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, {_, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Inherit attributes
def newAttrs = [:]
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { newAttrs[attr] = it }
}
newAttrs['target_iceberg_table_name'] = 'roles'
newAttrs['schema.name'] = 'roles'

newAttrs.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }
session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records to SUCCESS")

// If failure records exist, send a separate flowfile to REL_FAILURE
if (!failureRecords.isEmpty()) {
    FlowFile failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, {_, out ->
        out.write(JsonOutput.toJson(failureRecords).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', failureRecords.size().toString() )
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type', 'validation_summary')

    inheritedAttributes.each { attr ->
        flowFile.getAttribute(attr)?.with { failureFlowFile = session.putAttribute(failureFlowFile, attr, it) }
    }

    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${failureRecords.size()} validation errors to FAILURE")
}
