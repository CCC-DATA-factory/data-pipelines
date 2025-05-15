import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

// Logger and session
def log = log

def session = session

def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Fetch incoming FlowFile
FlowFile inputFlowFile = session.get()
if (!inputFlowFile) return

// Read full JSON
String inputJson = ''
inputFlowFile = session.write(inputFlowFile, { inputStream, outputStream ->
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

// Current time in Tunis timezone
long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()
List validRecords = []
List invalidRecords = []

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
        // Extract createdAtMillis
        Long createdAtMillis = record.createdAt instanceof Number ? record.createdAt : null
        if (createdAtMillis == null && record.first_seen_date instanceof Number) {
            createdAtMillis = (record.first_seen_date as Number).longValue()
        }
        if (createdAtMillis == null) {
            createdAtMillis = nowTunisMillis
        }

        // Derive year/month/day in Tunis timezone
        def dt = Instant.ofEpochMilli(createdAtMillis).atZone(ZoneId.of("Africa/Tunis"))
        int createdYear  = dt.getYear()
        int createdMonth = dt.getMonthValue()
        int createdDay   = dt.getDayOfMonth()

        // Normalize shopName
        def shop = (record.shopName ?: "Unknown").toString()

        def transformed = [
            id                  : record._id,
            retailer_id         : record.retailer,
            sim_id              : record.SIM,
            shopName            : shop,
            mvno_id             : record.MVNO ?: "",
            createdAt           : createdAtMillis,
            amount              : (record.reloadAmount.amount as Number).toDouble(),
            first_seen_date     : record.first_seen_date ?: null,
            ingestion_date      : record.ingestion_date ?: null,
            transformation_date : nowTunisMillis,
            source_system       : record.source_system ?: null,
            partition           : [ createdYear, createdMonth, createdDay, shop ]
        ]
        validRecords << transformed
    }
}

// Write success
if (!validRecords.isEmpty()) {
    FlowFile successFlowFile = session.create(inputFlowFile)
    successFlowFile = session.write(successFlowFile, { outputStream ->
        outputStream.write(JsonOutput.toJson(validRecords).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

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

// Write failures
if (!invalidRecords.isEmpty()) {
    FlowFile failureFlowFile = session.create(inputFlowFile)
    failureFlowFile = session.write(failureFlowFile, { outputStream ->
        outputStream.write(JsonOutput.toJson(invalidRecords).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, "error", "Found ${invalidRecords.size()} invalid records")
    session.transfer(failureFlowFile, REL_FAILURE)
    log.warn("Transferred ${invalidRecords.size()} invalid topup light records to failure")
}

// Remove original
session.remove(inputFlowFile)
