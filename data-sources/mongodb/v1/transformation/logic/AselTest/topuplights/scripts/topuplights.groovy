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
    } else if (!(record.reloadAmount?.amount instanceof Number)) {
        error = "reloadAmount.amount is missing or not a number"
    } else if (!record.retailer) {
        error = "retailer (retailer_id) is required"
    } else if (!record.SIM) {
        error = "SIM (sim_id) is required"
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
            retailer_id         : record.retailer,
            sim_id              : record.SIM,
            shopName            : record.shopName ?: "",
            mvno_id             : record.MVNO ?: "",
            createdAt           : createdAtMillis ?: nowTunisMillis,
            amount              : (record.reloadAmount.amount as Number).toDouble(),
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

    def validJsonBytes = JsonOutput.toJson(validRecords).getBytes(StandardCharsets.UTF_8)
    newAttributes['file.size'] = String.valueOf(validJsonBytes.length)
    newAttributes['records.count'] = String.valueOf(validRecords.size())
    newAttributes['target_iceberg_table_name'] = "topuplights"
    newAttributes['schema.name'] = "topuplights"

    newAttributes.each { k, v -> successFlowFile = session.putAttribute(successFlowFile, k, v) }
    session.transfer(successFlowFile, REL_SUCCESS)
    log.info("Transferred ${validRecords.size()} topup light records to success")
}

if (!invalidRecords.isEmpty()) {
    FlowFile failureFlowFile = session.create(inputFlowFile)
    failureFlowFile = session.write(failureFlowFile, { outputStream ->
        outputStream.write(JsonOutput.toJson(invalidRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, "error", "Found ${invalidRecords.size()} invalid records")
    session.transfer(failureFlowFile, REL_FAILURE)
    log.warn("Transferred ${invalidRecords.size()} invalid topup light records to failure")
}

session.remove(inputFlowFile)
