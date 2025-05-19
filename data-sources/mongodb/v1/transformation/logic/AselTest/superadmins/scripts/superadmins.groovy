import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

// Unchanged initialization
def log = log
def session = session
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

FlowFile inputFlowFile = session.get()
if (!inputFlowFile) return

// Read input JSON
String inputJson = ''
inputFlowFile = session.write(inputFlowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON list
def parser = new JsonSlurper()
def records
try {
    records = parser.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Input is not a list of JSON objects")
        inputFlowFile = session.putAttribute(inputFlowFile, "error", "Expected a list of JSON objects")
        session.transfer(inputFlowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    inputFlowFile = session.putAttribute(inputFlowFile, "error", "Invalid JSON format: ${e.message}")
    session.transfer(inputFlowFile, REL_FAILURE)
    return
}

long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Prepare combined output list
List outputRecords = []

records.eachWithIndex { record, idx ->
    def errors = []
    // Existing validation logic
    if (!(record._id instanceof String)) {
        errors << "_id must be a string"
    } else if (!(record.user instanceof String)) {
        errors << "user must be a string"
    }
    // Build base record map (merge original and transformed fields)
    def outRec = new LinkedHashMap<>(record)
    // Add transformation_date for all
    outRec['transformation_date'] = nowTunisMillis
    // Add additional fields for valid records
    if (errors.isEmpty()) {
        outRec['id'] = record._id
        outRec['id_user'] = record.user
        outRec['role'] = "superadmin"
        outRec['parent_id'] = null
        outRec['first_seen_date'] = record.first_seen_date ?: null
        outRec['ingestion_date'] = record.ingestion_date ?: null
        outRec['source_system'] = record.source_system ?: null
        outRec['partition'] =  null

    }
    // Attach validation metadata
    outRec['is_valid'] = errors.isEmpty()
    outRec['comment'] = errors.isEmpty() ? null : errors.join('; ')

    outputRecords << outRec
}

// Create output FlowFile for combined records
FlowFile outputFlowFile = session.create(inputFlowFile)
outputFlowFile = session.write(outputFlowFile, { _ , out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Inherit attributes
def newAttributes = [:]
inheritedAttributes.each { attr ->
    def val = inputFlowFile.getAttribute(attr)
    if (val != null) newAttributes[attr] = val
}
// Set metadata attributes
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
newAttributes['file.size'] = String.valueOf(jsonBytes.length)
newAttributes['records.count'] = String.valueOf(outputRecords.size())
newAttributes['target_iceberg_table_name'] = "roles"
newAttributes['schema.name'] = "roles"
newAttributes.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }

// Transfer and cleanup
session.transfer(outputFlowFile, REL_SUCCESS)
session.remove(inputFlowFile)
log.info("Transferred ${outputRecords.size()} records (valid & invalid) to success")
