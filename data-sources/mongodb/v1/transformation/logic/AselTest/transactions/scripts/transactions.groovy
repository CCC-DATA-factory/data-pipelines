import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

def log = log
def session = session
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

FlowFile inputFlowFile = session.get()
if (!inputFlowFile) return

String inputJson = ''
inputFlowFile = session.write(inputFlowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def parser = new JsonSlurper()
def records
try {
    records = parser.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Expected a list of JSON objects")
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

long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()
List validRecords = []
List invalidRecords = []

def parseDateMillis = { str ->
    try {
        return Instant.parse(str).toEpochMilli()
    } catch (Exception e) {
        return null
    }
}

records.eachWithIndex { record, idx ->
    def error = null

    if (!(record._id instanceof String)) {
        error = "_id must be a string"
    } else if (record.rejected_At != null) {
        error = "rejected_At is not null → rejected record"
    } else if (!(record.sender instanceof String)) {
        error = "sender must be a string"
    } else if (!(record.recipient instanceof String)) {
        error = "recipient must be a string"
    } else if (!(record.transactionAmount?.amount instanceof Number)) {
        error = "transactionAmount.amount must be a number"
    }

    if (error) {
        record['_error'] = error
        record['_index'] = idx
        invalidRecords << record
        log.warn("Invalid record at index ${idx}: ${error}")
    } else {
        def createdAtMillis = parseDateMillis(record.createdAt)
        if (createdAtMillis == null && record.first_seen_date instanceof Number) {
            createdAtMillis = record.first_seen_date
        }

        def transformed = [
            id                  : record._id,
            sender_id           : record.sender,
            recipient_id        : record.recipient,
            transactionAmount   : (record.transactionAmount.amount as Number).toDouble(),
            createdAt           : createdAtMillis ?: nowTunisMillis,
            first_seen_date     : record.first_seen_date ?: null,
            ingestion_date      : record.ingestion_date ?: null,
            transformation_date : nowTunisMillis,
            source_system       : record.source_system ?: null
        ]
        validRecords << transformed
    }
}

if (!validRecords.isEmpty()) {
    FlowFile successFlowFile = session.create(inputFlowFile)
    successFlowFile = session.write(successFlowFile, { outputStream ->
        outputStream.write(JsonOutput.toJson(validRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    def newAttributes = [:]
    inheritedAttributes.each { attr ->
        def val = inputFlowFile.getAttribute(attr)
        if (val != null) newAttributes[attr] = val
    }

    newAttributes['file.size'] = validRecords.size().toString()
    newAttributes['records.count'] = validRecords.size().toString()
    newAttributes['target_iceberg_table_name'] = "transactions"
    newAttributes['schema.name'] = "transactions"

    newAttributes.each { k, v -> successFlowFile = session.putAttribute(successFlowFile, k, v) }
    session.transfer(successFlowFile, REL_SUCCESS)
    log.info("Transferred ${validRecords.size()} transaction records to success")
}

if (!invalidRecords.isEmpty()) {
    FlowFile failureFlowFile = session.create(inputFlowFile)
    failureFlowFile = session.write(failureFlowFile, { outputStream ->
        outputStream.write(JsonOutput.toJson(invalidRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, "error", "Found ${invalidRecords.size()} invalid records")
    session.transfer(failureFlowFile, REL_FAILURE)
    log.warn("Transferred ${invalidRecords.size()} invalid transaction records to failure")
}

session.remove(inputFlowFile)
