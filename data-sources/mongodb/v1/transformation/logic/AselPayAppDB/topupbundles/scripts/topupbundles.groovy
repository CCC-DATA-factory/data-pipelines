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

long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Prepare combined output list
List outputRecords = []

records.eachWithIndex { record, idx ->
    def errors = []
    // Original validation logic
    if (!(record._id instanceof String)) {
        errors << "_id must be a string"
    } else if (!(record.retailer instanceof String)) {
        errors << "retailer must be a string"
    } else if (!(record.bundle instanceof String)) {
        errors << "bundle must be a string"
    } else if (!(record.SIM instanceof String)) {
        errors << "SIM must be a string"
    }

    def createdAtMillis = null
    if (record.createdAt instanceof Number) {
        createdAtMillis = (record.createdAt as Number).longValue()
    } else if (record.first_seen_date instanceof Number) {
        createdAtMillis = (record.first_seen_date as Number).longValue()
    } else {
        // fallback to now if neither present
        createdAtMillis = nowTunisMillis
    }

    // Original transform logic for valid records
    def transformed = [:]
    if (errors.isEmpty()) {
        def dt = Instant.ofEpochMilli(createdAtMillis).atZone(ZoneId.of("Africa/Tunis"))
        def shop = (record.shopName ?: "Unknown").toString()
        transformed = [
            id                  : record._id,
            retailer_id         : record.retailer,
            sim_id              : record.SIM,
            bundle_id           : record.bundle,
            shopName            : shop,
            mvno_id             : record.MVNO ?: null,
            createdAt           : createdAtMillis,
            first_seen_date     : record.first_seen_date ?: null,
            ingestion_date      : record.ingestion_date ?: null,
            transformation_date : nowTunisMillis,
            source_system       : record.source_system ?: null,
            created_year        : dt.getYear(),
            created_month       : dt.getMonthValue(),
            created_day         : dt.getDayOfMonth(),
            partition           : [ dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth(), shop ],
            is_valid            : errors.isEmpty(),
            comment             : errors.isEmpty() ? null : errors.join('; ')
        ]
    }

    outputRecords << transformed
}

// Create new FlowFile for combined output
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, { _ , out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Inherit attributes
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { outputFlowFile = session.putAttribute(outputFlowFile, attr, it) }
}

// Add metadata attributes
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
outputFlowFile = session.putAttribute(outputFlowFile, 'file.size', jsonBytes.length.toString())
outputFlowFile = session.putAttribute(outputFlowFile, 'records.count', outputRecords.size().toString())
outputFlowFile = session.putAttribute(outputFlowFile, 'target_iceberg_table_name', 'topupbundles')
outputFlowFile = session.putAttribute(outputFlowFile, 'schema.name', 'topupbundles')

// Transfer combined output and remove original
session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records (valid & invalid) with validation flags")
