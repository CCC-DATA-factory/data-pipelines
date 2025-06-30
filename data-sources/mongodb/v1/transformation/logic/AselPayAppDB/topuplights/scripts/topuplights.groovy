import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Read full JSON
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

// Current time in Tunis timezone
long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()
def dbName = flowFile.getAttribute("database_name") ?: "unknown_database"
def collName = flowFile.getAttribute("collection_name") ?: "unknown_collection"

List outputRecords = []
List invalidSummary = []

// Date parser
def parseDateMillis = { str ->
    try {
        return Instant.parse(str).toEpochMilli()
    } catch (Exception e) {
        return null
    }
}

records.eachWithIndex { record, idx ->
    def errors = []

    def id = (record._id instanceof String) ? record._id : null
    def retailer = (record.retailer instanceof String) ? record.retailer : null
    def sim = (record.SIM instanceof String) ? record.SIM : null
    def amount = (record.reloadAmount?.amount instanceof Number) ? (record.reloadAmount.amount as Number).toDouble() : null
    def shop = (record.shopName ?: "Unknown").toString()

    if (!id) errors << "_id must be a string"
    if (amount == null) errors << "reloadAmount.amount is missing or not a number"
    if (!retailer) errors << "retailer must be a string"
    if (!sim && (!record.SN || !record.MSISDN)) {
        errors << "SIM must be a string or (SN and MSISDN) must be present"
    }

    // Parse createdAt
    Long createdAtMillis = null
    if (record.createdAt instanceof String) {
        createdAtMillis = parseDateMillis(record.createdAt)
    }
    if (!createdAtMillis && record.first_seen_date instanceof Number) {
        createdAtMillis = (record.first_seen_date as Number).longValue()
    }
    if (!createdAtMillis) {
        createdAtMillis = nowTunisMillis
    }

    def dt = Instant.ofEpochMilli(createdAtMillis).atZone(ZoneId.of("Africa/Tunis"))

    def transformed = [
        id                  : id,
        retailer_id         : retailer,
        sim_id              : sim,
        msisdn              : record.MSISDN ?: null,
        sn                  : record.SN ?: null,
        shop_name           : record.shopName ?: null,
        mvno_id             : record.MVNO ?: null,
        created_at          : createdAtMillis,
        amount              : amount,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null,
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join('; '),
    ]

    outputRecords << transformed

    if (!errors.isEmpty()) {
        log.warn("Invalid record at index ${idx}: ${errors.join('; ')}")
        invalidSummary << [
            database_name   : dbName,
            collection_name : collName,
            record_id       : id,
            error_message   : errors.join('; ')
        ]
    }
}

// Write combined output (all records) to success
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, { _, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Copy and set attributes
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
def newAttrs = [
    'target_iceberg_table_name': "topuplights",
    'schema.name'              : "topuplights"
]
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { newAttrs[attr] = it }
}
newAttrs.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }

session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} topuplight records with validation flags")

// If invalid records exist, send summary to REL_FAILURE
if (!invalidSummary.isEmpty()) {
    FlowFile failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { _, out ->
        out.write(JsonOutput.toJson(invalidSummary).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, "error.count", "${invalidSummary.size().toString()}")
    failureFlowFile = session.putAttribute(failureFlowFile, "error.type", "validation_summary")
    inheritedAttributes.each { attr ->
        flowFile.getAttribute(attr)?.with {
            failureFlowFile = session.putAttribute(failureFlowFile, attr, it)
        }
    }

    session.transfer(failureFlowFile, REL_FAILURE)
    log.warn("Validation errors found in ${invalidSummary.size()} topuplight records")
}
