import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Read content as JSON string
String inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON array
def parser = new JsonSlurper()
def records
try {
    records = parser.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Input is not a list of JSON objects")
        session.transfer(
            session.putAttribute(flowFile, "error", "Expected a list of JSON objects"),
            REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    session.transfer(
        session.putAttribute(flowFile, "error", "Invalid JSON format: ${e.message}"),
        REL_FAILURE)
    return
}

// Current timestamp in Tunis timezone
long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Prepare combined output list
List outputRecords = []

records.eachWithIndex { record, idx ->
    def errors = []

    // Validate required fields and check type string
    def id = (record._id instanceof String) ? record._id : null
    def user = (record.user instanceof String) ? record.user : null
    if (!id) errors << "_id missing or invalid"
    if (!user) errors << "user missing or invalid"

    def transformed = [
        id                  : id,
        id_user             : user,
        role                : "sub-distributor",
        parent_id           : record.subWholesaler ?: null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null,
        partition           : null,
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join('; ')
    ]

    outputRecords << transformed

    if (!errors.isEmpty()) {
        log.warn("Invalid record at index ${idx}: ${errors.join('; ')}")
    }
}

// Write combined records to single FlowFile
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, { _, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Carry forward inherited attributes and add metadata
def newAttrs = [:]
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { newAttrs[attr] = it }
}
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
newAttrs['file.size'] = jsonBytes.length.toString()
newAttrs['records.count'] = outputRecords.size().toString()
newAttrs['target_iceberg_table_name'] = "roles"
newAttrs['schema.name'] = "roles"
newAttrs.each { k, v ->
    outputFlowFile = session.putAttribute(outputFlowFile, k, v)
}

session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records with validation flags")
