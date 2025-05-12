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

def parseDateMillis = { str ->
    try {
        return Instant.parse(str).toEpochMilli()
    } catch (Exception e) {
        return null
    }
}

long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

List validRecords = []
List invalidRecords = []

records.eachWithIndex { record, idx ->
    def error = null

    // Required fields
    if (!(record._id instanceof String)) {
        error = "_id must be a string"
    } else if (!(record.ICCID instanceof String)) {
        error = "ICCID must be a string"
    } else if (!(record.SN instanceof String)) {
        error = "SN must be a string"
    } else if (!(record.MSISDN instanceof String)) {
        error = "MSISDN must be a string"
    } else if (!(record.IMSI instanceof String)) {
        error = "IMSI must be a string"
    } else if (!(record.mvno instanceof String)) {
        error = "mvno must be a string"
    }

    if (error) {
        record['_error'] = error
        record['_index'] = idx
        invalidRecords << record
        log.warn("Invalid SimRecord at index ${idx}: ${error}")
    } else {
        def activationMillis = parseDateMillis(record.activation_date)
        if (!activationMillis && record.first_seen_date instanceof Number) {
            activationMillis = record.first_seen_date
        }

        def transformed = [
            id                  : record._id,
            ICCID               : record.ICCID,
            SN                  : record.SN,
            MSISDN              : record.MSISDN,
            IMSI                : record.IMSI,
            mvno_id             : record.mvno,
            activation_date     : activationMillis ?: nowMillis,
            customer_id         : "unknown", // placeholder if customer_id not present in original schema
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

    successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "sim")
    successFlowFile = session.putAttribute(successFlowFile, "schema.name", "sim")
    successFlowFile = session.putAttribute(successFlowFile, "record.count", validRecords.size().toString())
    session.transfer(successFlowFile, REL_SUCCESS)
    log.info("Transferred ${validRecords.size()} sim records to success")
}

if (!invalidRecords.isEmpty()) {
    def failureFlowFile = session.create(inputFlowFile)
    failureFlowFile = session.write(failureFlowFile, { out ->
        out.write(JsonOutput.toJson(invalidRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, "error", "Found ${invalidRecords.size()} invalid sim records")
    session.transfer(failureFlowFile, REL_FAILURE)
    log.warn("Transferred ${invalidRecords.size()} invalid sim records to failure")
}

session.remove(inputFlowFile)
