import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

def session = session
def log = log

FlowFile inputFlowFile = session.get()
if (!inputFlowFile) return

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
        log.error("Expected a list of objects but got: " + records.getClass().getName())
        session.transfer(inputFlowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    session.transfer(inputFlowFile, REL_FAILURE)
    return
}

long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

List validRecords = []
List invalidRecords = []

records.eachWithIndex { record, idx ->
    def error = null

    if (!(record._id instanceof String)) {
        error = "_id must be a string"
    } else if (!(record.user instanceof String)) {
        error = "user must be a string"
    }

    if (error) {
        record['_error'] = error
        record['_index'] = idx
        invalidRecords << record
        log.warn("Invalid WholesalerRecord at index ${idx}: ${error}")
    } else {
        def transformed = [
            id                  : record._id,
            id_user             : record.user,
            role                : "wholesaler",
            parent_id           : record.superAdmin ?: null,
            first_seen_date     : record.first_seen_date ?: null,
            ingestion_date      : record.ingestion_date ?: null,
            transformation_date : nowMillis,
            source_system       : record.source_system ?: null
        ]
        validRecords << transformed
    }
}

if (!validRecords.isEmpty()) {
    def successFlowFile = session.create(inputFlowFile)
    successFlowFile = session.write(successFlowFile, { out ->
        out.write(JsonOutput.toJson(validRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "processed_role")
    successFlowFile = session.putAttribute(successFlowFile, "schema.name", "processed_role")
    successFlowFile = session.putAttribute(successFlowFile, "record.count", validRecords.size().toString())
    session.transfer(successFlowFile, REL_SUCCESS)
    log.info("Transferred ${validRecords.size()} records to success")
}

if (!invalidRecords.isEmpty()) {
    def failureFlowFile = session.create(inputFlowFile)
    failureFlowFile = session.write(failureFlowFile, { out ->
        out.write(JsonOutput.toJson(invalidRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, "error", "Found ${invalidRecords.size()} invalid records")
    session.transfer(failureFlowFile, REL_FAILURE)
    log.warn("Transferred ${invalidRecords.size()} invalid records to failure")
}

session.remove(inputFlowFile)
