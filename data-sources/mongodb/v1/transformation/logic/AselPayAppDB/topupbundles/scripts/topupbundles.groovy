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


def parseLongSafe = { obj ->
    try {
        return obj?.toString()?.toLong()
    } catch (_) {
        return null
    }
}
long utcToTunisLocalEpochMillis(long utcEpochMillis) {
    ZoneId tunisZone = ZoneId.of("Africa/Tunis")

    // Convert UTC millis to Instant
    Instant instant = Instant.ofEpochMilli(utcEpochMillis)

    // Get the ZonedDateTime in Tunisia timezone for that instant
    ZonedDateTime tunisZoned = instant.atZone(tunisZone)

    // Get local date/time components
    LocalDateTime tunisLocalDateTime = tunisZoned.toLocalDateTime()

    // Now interpret that local date/time as if it were UTC, get the epoch millis
    long shiftedMillis = tunisLocalDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli()

    return shiftedMillis
}

records.eachWithIndex { record, idx ->
    def errors = []

    // Validate required fields
    def id = (record._id instanceof String) ? record._id : null
    def retailer = (record.retailer instanceof String) ? record.retailer : null
    def bundle = (record.bundle instanceof String) ? record.bundle : null
    def sim = (record.SIM instanceof String) ? record.SIM : null
    def shop = (record.shopName ?: "Unknown").toString()

    if (!id) errors << "_id must be a string"
    if (!retailer) errors << "retailer must be a string"
    if (!bundle) errors << "bundle must be a string"
    if (!sim && (!record.SN || !record.MSISDN)) {
        errors << "SIM must be a string or (SN and MSISDN) must be present"
    }

    // Parse createdAt or fallback
    Long createdAtMillis = null

    if (record.createdAt != null) {
        def createdAtLong = parseLongSafe(record.createdAt)
        if (createdAtLong != null) {
            createdAtMillis = utcToTunisLocalEpochMillis(createdAtLong)
        }
    } else if (record.first_seen_date != null) {
        def firstSeenLong = parseLongSafe(record.first_seen_date)
        if (firstSeenLong != null) {
            createdAtMillis = firstSeenLong
        }
    }

    if (createdAtMillis == null) {
        createdAtMillis = nowTunisMillis
    }


 

    def transformed = [
        id                  : id,
        retailer_id         : retailer,
        sim_id              : sim,
        msisdn              : record.MSISDN ?: null,
        sn                  : record.SN ?: null,
        bundle_id           : bundle,
        shop_name           : shop,
        mvno_id             : record.MVNO ?: null,
        created_at          : createdAtMillis,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null,
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join('; ')
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

// Write output FlowFile with all records
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, { _, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Copy inherited attributes and set metadata
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
def newAttrs = [
    'target_iceberg_table_name': "topupbundles",
    'schema.name'              : "topupbundles"
]
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { newAttrs[attr] = it }
}
newAttrs.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }

session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} topupbundle records with validation flags")

// If validation issues exist, write to REL_FAILURE
if (!invalidSummary.isEmpty()) {
    FlowFile failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { _, out ->
        out.write(JsonOutput.toJson(invalidSummary).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, "error.count", invalidSummary.size().toString())
    failureFlowFile = session.putAttribute(failureFlowFile, "error.type", "validation_summary")
    inheritedAttributes.each { attr ->
        flowFile.getAttribute(attr)?.with {
            failureFlowFile = session.putAttribute(failureFlowFile, attr, it)
        }
    }

    session.transfer(failureFlowFile, REL_FAILURE)
    log.warn("Validation errors found in ${invalidSummary.size()} topupbundle records")
}
