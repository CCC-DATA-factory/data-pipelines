import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.Relationship

// Initialize session and logger
def session = session
def log = log
// Attributes to inherit
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Fetch FlowFile
FlowFile inputFlowFile = session.get()
if (!inputFlowFile) return

// Read input JSON from FlowFile
String inputJson = ''
inputFlowFile = session.write(inputFlowFile, { inputStream, outputStream ->
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
        session.transfer(session.putAttribute(inputFlowFile, "error", "Expected a list of JSON objects"), REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    session.transfer(session.putAttribute(inputFlowFile, "error", "Invalid JSON format: ${e.message}"), REL_FAILURE)
    return
}

// Current timestamp in Africa/Tunis timezone
long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Combine valid and invalid records into a single list with validation attributes
List outputRecords = []
records.eachWithIndex { record, idx ->
    def errors = []
    // Validate _id
    if (!(record._id instanceof String)) {
        errors << "_id must be a string"
    }
    // Validate user
    if (!(record.user instanceof String)) {
        errors << "user must be a string"
    }

    // Prepare transformed fields if valid
    def transformed = [:]
    if (errors.isEmpty()) {
        def parentId = record.subWholesaler ?: record.subDistributor ?: record.wholesaler ?: null
        transformed = [
            id                  : record._id,
            id_user             : record.user,
            role                : "retailer",
            parent_id           : parentId,
            first_seen_date     : record.first_seen_date ?: null,
            ingestion_date      : record.ingestion_date ?: null,
            transformation_date : nowTunisMillis,
            source_system       : record.source_system ?: null,
            partition           : null
        ]
    } else {
        // Log each validation error
        errors.each { err -> log.warn("Invalid record at index ${idx}: ${err}") }
    }

    // Merge original and transformed data, and add validation attributes
    def outRec = new LinkedHashMap<>(record)
    transformed.each { k, v -> outRec[k] = v }
    outRec['is_valid'] = errors.isEmpty()
    outRec['comment'] = errors.isEmpty() ? null : errors.join('; ')
    outputRecords << outRec
}

// Create output FlowFile with combined records
FlowFile outputFlowFile = session.create(inputFlowFile)
outputFlowFile = session.write(outputFlowFile, {_, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Inherit selected attributes
def newAttrs = [:]
inheritedAttributes.each { attr ->
    inputFlowFile.getAttribute(attr)?.with { newAttrs[attr] = it }
}
// Add metadata attributes
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
newAttrs['file.size'] = String.valueOf(jsonBytes.length)
newAttrs['records.count'] = String.valueOf(outputRecords.size())
newAttrs['target_iceberg_table_name'] =  'roles'
newAttrs['schema.name'] = 'roles'
newAttrs.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }

// Transfer the FlowFile to success relationship
session.transfer(outputFlowFile, REL_SUCCESS)
// Remove original FlowFile
session.remove(inputFlowFile)
log.info("Transferred ${outputRecords.size()} records with validation flags (combined)")