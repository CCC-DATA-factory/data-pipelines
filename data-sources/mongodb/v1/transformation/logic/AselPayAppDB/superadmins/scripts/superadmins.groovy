import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Read input JSON
String inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
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
        flowFile = session.putAttribute(flowFile, "error", "Expected a list of JSON objects")
        session.transfer(flowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    flowFile = session.putAttribute(flowFile, "error", "Invalid JSON format: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Prepare combined output list
List outputRecords = []

records.eachWithIndex { record, idx ->
    def errors = []

    def id = (record._id instanceof String) ? record._id : null
    def user = (record.user instanceof String) ? record.user : null
    if (!id) errors << "_id missing or invalid"
    if (!user) errors << "user missing or invalid"

    def transformed = [
        id                  : id,
        id_user             : user,
        role                : "superadmin",
        parent_id           : null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        source_system       : record.source_system ?: null,
        partition           : null,
        transformation_date : nowTunisMillis,
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join('; ')
    ]

    if (!errors.isEmpty()) {
        log.warn("Invalid record at index ${idx}: ${errors.join('; ')}")
    }

    outputRecords << transformed
}

// Create output FlowFile for combined records
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, { _, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Inherit attributes
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { outputFlowFile = session.putAttribute(outputFlowFile, attr, it) }
}

// Set metadata attributes
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
outputFlowFile = session.putAttribute(outputFlowFile, 'file.size', String.valueOf(jsonBytes.length))
outputFlowFile = session.putAttribute(outputFlowFile, 'records.count', String.valueOf(outputRecords.size()))
outputFlowFile = session.putAttribute(outputFlowFile, 'target_iceberg_table_name', 'roles')
outputFlowFile = session.putAttribute(outputFlowFile, 'schema.name', 'roles')

// Transfer and cleanup
session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records (valid & invalid) to success")
