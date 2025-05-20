import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.flowfile.FlowFile


def inheritedAttributes = ['filepath', 'database_name', 'collection_name']



// Read input JSON safely
String inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

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
List outputRecords = []

records.eachWithIndex { record, idx ->
    def error = null

    if (!(record._id instanceof String)) {
        error = "_id must be a string"
    } else if (!(record.firstName instanceof String)) {
        error = "firstName must be a string"
    } else if (!(record.lastName instanceof String)) {
        error = "lastName must be a string"
    }

    def joinedMillis = (record.createdAt ?: record.first_seen_date) as Long
    if (!joinedMillis) {
        error = "Missing createdAt or first_seen_date"
    }

    if (error) {
        record['is_valid'] = false
        record['comment'] = error
        outputRecords << record
        log.warn("Invalid record at index ${idx}: ${error}")
    } else {
        def transformed = [
            id                  : record._id,
            name                : "${record.firstName} ${record.lastName}",
            shop_name           : record.shopName ?: null,
            joined_at           : joinedMillis,
            first_seen_date     : record.first_seen_date ?: null,
            ingestion_date      : record.ingestion_date ?: null,
            transformation_date : nowTunisMillis,
            source_system       : record.source_system ?: null,
            is_valid            : true,
            comment             : null,
            partition           : null
        ]
        outputRecords << transformed
    }
}

// Use OutputStreamCallback (1-arg) to avoid MissingMethodException
FlowFile successFlowFile = session.create(flowFile)
successFlowFile = session.write(successFlowFile, { outputStream ->
    outputStream.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as OutputStreamCallback)

// Copy attributes and add metadata
def newAttributes = [:]
inheritedAttributes.each { attr ->
    def val = flowFile.getAttribute(attr)
    if (val != null) newAttributes[attr] = val
}

def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
newAttributes['file.size'] = String.valueOf(jsonBytes.length)
newAttributes['records.count'] = String.valueOf(outputRecords.size())
newAttributes['target_iceberg_table_name'] = "users"
newAttributes['schema.name'] = "users"

newAttributes.each { k, v -> successFlowFile = session.putAttribute(successFlowFile, k, v) }

session.transfer(successFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} total records to success (including valid & invalid)")

