import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.Relationship

// Attributes to inherit
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

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

// Prepare output list
List outputRecords = []

// Iterate and validate/transform records
records.eachWithIndex { record, idx ->
    def errors = []

    // Initialize all fields with nulls or default values
    def transformed = [
        id                  : record._id ?: null,
        id_user             : record.user ?: null,
        role                : "admin",
        parent_id           : null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null,
        partition           : null
    ]

    // Field validations (only add errors, don't block partial data)
    if (!(record._id instanceof String)) errors << "_id must be a string"
    if (!(record.user instanceof String)) errors << "user must be a string"

    def outRec = new LinkedHashMap<>(transformed)
    outRec['is_valid'] = errors.isEmpty()
    outRec['comment'] = errors.isEmpty() ? null : errors.join('; ')

    outputRecords << outRec
}

// Create new FlowFile for output
FlowFile outputFlowFile = session.create(flowFile)
// Write combined JSON
outputFlowFile = session.write(outputFlowFile, {_, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Inherit attributes
def newAttrs = [:]
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { newAttrs[attr] = it }
}

// Add metadata
newAttrs['file.size'] = String.valueOf(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8).length)
newAttrs['records.count'] = String.valueOf(outputRecords.size())
newAttrs['target_iceberg_table_name'] = 'roles'
newAttrs['schema.name'] = 'roles'

newAttrs.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }

session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records with validation flags (valid and invalid combined)")
