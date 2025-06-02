import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

// Attributes to inherit
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Read input JSON from FlowFile
String inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON into records list
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

// Current timestamp in Africa/Tunis timezone
long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Combined records + error summary
List outputRecords = []
List invalidRecordsSummary = []

def databaseName = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collectionName = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

records.eachWithIndex { record, idx ->
    def errors = []

    if (!(record._id instanceof String)) {
        errors << "_id must be a string"
    }
    if (!(record.user instanceof String)) {
        errors << "user must be a string"
    }

    def transformed = [
        id                  : record._id ?: null,
        id_user             : record.user ?: null,
        role                : "retailer",
        parent_id           : record.subWholesaler ?: record.subDistributor ?: record.wholesaler ?: null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join("; ")
    ]

    outputRecords << transformed

    if (!errors.isEmpty()) {
        invalidRecordsSummary << [
            database_name   : databaseName,
            collection_name : collectionName,
            record_id       : record._id?.toString() ?: null,
            error_message   : errors.join("; ")
        ]
    }
}

// Write valid + invalid records to REL_SUCCESS
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, { _, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def newAttrs = [:]
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { newAttrs[attr] = it }
}
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
newAttrs['target_iceberg_table_name'] = 'roles'
newAttrs['schema.name'] = 'roles'
newAttrs.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }

session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records with validation flags (combined)")

// If any validation errors exist, write to REL_FAILURE as well
if (!invalidRecordsSummary.isEmpty()) {
    FlowFile failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { _, out ->
        out.write(JsonOutput.toJson(invalidRecordsSummary).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', invalidRecordsSummary.size().toString())
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type', 'validation_summary')
    inheritedAttributes.each { attr ->
        flowFile.getAttribute(attr)?.with {
            failureFlowFile = session.putAttribute(failureFlowFile, attr, it)
        }
    }

    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${invalidRecordsSummary.size()} validation errors to REL_FAILURE")
}
