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

List outputRecords = []

def parseDateMillis = { str ->
    try {
        return Instant.parse(str).toEpochMilli()
    } catch (Exception e) {
        return null
    }
}

records.eachWithIndex { record, idx ->
    def errors = []

    // Validate required fields
    if (!(record._id instanceof String)) {
        errors << "_id must be a string"
    }
    if (!(record.reloadAmount?.amount instanceof Number)) {
        errors << "reloadAmount.amount is missing or not a number"
    }
    if (!(record.retailer instanceof String)) {
        errors << "retailer must be a string"
    }
    if (!(record.SIM instanceof String)) {
        errors << "SIM must be a string"
    }

    // Parse createdAt or fallback
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
    int createdYear = dt.getYear()
    int createdMonth = dt.getMonthValue()
    int createdDay = dt.getDayOfMonth()
    String shop = (record.shopName ?: "Unknown").toString()

    // Build unified output schema
    def transformed = [
        id                  : (record._id instanceof String) ? record._id : null,
        retailer_id         : (record.retailer instanceof String) ? record.retailer : null,
        sim_id              : (record.SIM instanceof String) ? record.SIM : null,
        shop_name            : record.shopName ?: null,
        mvno_id             : record.MVNO ?: null,
        created_at           : createdAtMillis,
        amount              : (record.reloadAmount?.amount instanceof Number) ? (record.reloadAmount.amount as Number).toDouble() : null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null,
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join('; '),
        partition           : [createdYear, createdMonth, createdDay, shop]
    ]

    outputRecords << transformed
}

// Write combined output to success
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, { _ , out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Inherit and set attributes
def newAttributes = [:]
inheritedAttributes.each { attr ->
    def val = flowFile.getAttribute(attr)
    if (val) newAttributes[attr] = val
}

def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
newAttributes['file.size'] = String.valueOf(jsonBytes.length)
newAttributes['records.count'] = String.valueOf(outputRecords.size())
newAttributes['target_iceberg_table_name'] = "topuplights"
newAttributes['schema.name'] = "topuplights"
newAttributes.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }

session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records with validation flags (valid and invalid combined)")
