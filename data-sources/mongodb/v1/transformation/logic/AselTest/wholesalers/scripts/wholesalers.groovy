import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.flowfile.FlowFile

def session = session
def log = log

FlowFile inputFlowFile = session.get()
if (!inputFlowFile) return

// Read input JSON safely using 2-arg closure
def inputJson = ''
inputFlowFile = session.write(inputFlowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def jsonSlurper = new JsonSlurper()
def records
try {
    records = jsonSlurper.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Expected a list of objects but got: ${records.getClass().getName()}")
        inputFlowFile = session.putAttribute(inputFlowFile, "error", "Input is not a list")
        session.transfer(inputFlowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    inputFlowFile = session.putAttribute(inputFlowFile, "error", "Invalid JSON: ${e.message}")
    session.transfer(inputFlowFile, REL_FAILURE)
    return
}

long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()
List mergedRecords = []

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
        comment             : isValid ? null : error,
        partition           : null
    ]
    mergedRecords << transformed
}

// Write result safely using OutputStreamCallback (1-arg)
if (!mergedRecords.isEmpty()) {
    def successFlowFile = session.create(inputFlowFile)
    successFlowFile = session.write(successFlowFile, { out ->
        out.write(JsonOutput.toJson(mergedRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "roles")
    successFlowFile = session.putAttribute(successFlowFile, "schema.name", "roles")
    successFlowFile = session.putAttribute(successFlowFile, "record.count", mergedRecords.size().toString())

    session.transfer(successFlowFile, REL_SUCCESS)
    log.info("Transferred ${mergedRecords.size()} records (valid + invalid) to success")
}

session.remove(inputFlowFile)
