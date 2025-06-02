import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.flowfile.FlowFile

// Attributes to inherit for failure FlowFile
def inheritedAttributes = ['database_name', 'collection_name']

// Get database_name and collection_name from flowFile attributes
def database_name = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collection_name = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

// Read input JSON safely using 2-arg closure
def inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def jsonSlurper = new JsonSlurper()
def records
try {
    records = jsonSlurper.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Expected a list of objects but got: ${records.getClass().getName()}")
        flowFile = session.putAttribute(flowFile, "error", "Input is not a list")
        session.transfer(flowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    flowFile = session.putAttribute(flowFile, "error", "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()
List mergedRecords = []
List failureRecords = []

records.eachWithIndex { record, idx ->
    def error = null
    def isValid = true

    if (!(record._id instanceof String)) {
        error = "_id must be a string"
        isValid = false
    } else if (!(record.user instanceof String)) {
        error = "user must be a string"
        isValid = false
    }

    if (!isValid) {
        log.warn("Invalid WholesalerRecord at index ${idx}: ${error}")
        // Add to failure records list
        failureRecords << [
            database_name   : database_name,
            collection_name : collection_name,
            record_id       : record._id ?: null,
            error_message   : error
        ]
    }

    def transformed = [
        id                  : record._id ?: null,
        id_user             : record.user ?: null,
        role                : "wholesaler",
        parent_id           : record.superAdmin ?: null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : record.source_system ?: null,
        is_valid            : isValid,
        comment             : isValid ? null : error
    ]
    mergedRecords << transformed
}

// Write success FlowFile with all records (valid + invalid)
if (!mergedRecords.isEmpty()) {
    def successFlowFile = session.create(flowFile)
    successFlowFile = session.write(successFlowFile, { out ->
        out.write(JsonOutput.toJson(mergedRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "roles")
    successFlowFile = session.putAttribute(successFlowFile, "schema.name", "roles")
    successFlowFile = session.putAttribute(successFlowFile, "record.count", mergedRecords.size().toString())

    session.transfer(successFlowFile, REL_SUCCESS)
    log.info("Transferred ${mergedRecords.size()} records (valid + invalid) to success")
}

// Write failure FlowFile with error summary if any failures occurred
if (!failureRecords.isEmpty()) {
    def failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { out ->
        out.write(JsonOutput.toJson(failureRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', failureRecords.size().toString())
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type', 'validation_summary')

    // Inherit database and collection attributes
    inheritedAttributes.each { attr ->
        flowFile.getAttribute(attr)?.with { failureFlowFile = session.putAttribute(failureFlowFile, attr, it) }
    }

    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${failureRecords.size()} validation errors to failure")
}
