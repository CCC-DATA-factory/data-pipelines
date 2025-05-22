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

records.eachWithIndex { record, idx ->
    def errors = []

    // Validate required fields
    if (!(record._id instanceof String)) {
        errors << "_id must be a string"
    }
    if (!(record.retailer instanceof String)) {
        errors << "retailer must be a string"
    }
    if (!(record.bundle instanceof String)) {
        errors << "bundle must be a string"
    }
    if (!(record.SIM instanceof String)) {
        errors << "SIM must be a string"
    }

    // Parse or default createdAt
    Long createdAtMillis = null
    if (record.createdAt instanceof Number) {
        createdAtMillis = (record.createdAt as Number).longValue()
    } else if (record.first_seen_date instanceof Number) {
        createdAtMillis = (record.first_seen_date as Number).longValue()
    } else {
        createdAtMillis = nowTunisMillis
    }

    // Derive date parts and normalize shopName
    def dt = Instant.ofEpochMilli(createdAtMillis).atZone(ZoneId.of("Africa/Tunis"))
    int createdYear = dt.getYear()
    int createdMonth = dt.getMonthValue()
    int createdDay = dt.getDayOfMonth()
    String shop = (record.shopName ?: "Unknown").toString()

    // Build transformed fields
    def transformed = [
        id                  : (record._id instanceof String) ? record._id : null,
        retailer_id         : (record.retailer instanceof String) ? record.retailer : null,
        sim_id              : (record.SIM instanceof String) ? record.SIM : null,
        bundle_id           : (record.bundle instanceof String) ? record.bundle : null,
        shopName            : shop,
        mvno_id             : record.MVNO ?: null,
        createdAt           : createdAtMillis,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null,
        created_year        : createdYear,
        created_month       : createdMonth,
        created_day         : createdDay,
        partition           : [createdYear, createdMonth, createdDay, shop],
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join('; ')
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
newAttributes['target_iceberg_table_name'] = "topupbundles"
newAttributes['schema.name'] = "topupbundles"
newAttributes.each { k, v -> outputFlowFile = session.putAttribute(outputFlowFile, k, v) }

session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records with validation flags (valid and invalid combined)")
