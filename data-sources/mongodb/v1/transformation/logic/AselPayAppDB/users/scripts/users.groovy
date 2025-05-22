import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
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

    def id = record._id instanceof String ? record._id : null
    def firstName = record.firstName instanceof String ? record.firstName : null
    def lastName = record.lastName instanceof String ? record.lastName : null
    def shopName = record.shopName ?: null
    def firstSeenDate = record.first_seen_date ?: null
    def ingestionDate = record.ingestion_date ?: null
    def sourceSystem = record.source_system ?: null

    def joinedMillis = null
    if (record.createdAt instanceof Number) {
        joinedMillis = record.createdAt
    } else if (record.first_seen_date instanceof Number) {
        joinedMillis = record.first_seen_date
    }

    if (!id) {
        error = "_id must be a string"
    } else if (!firstName) {
        error = "firstName must be a string"
    } else if (!lastName) {
        error = "lastName must be a string"
    } else if (joinedMillis == null) {
        error = "Missing createdAt or first_seen_date"
    }

    def outputRecord = [
        id                  : id,
        name                : (firstName && lastName) ? "${firstName} ${lastName}" : null,
        shop_name           : shopName,
        joined_at           : joinedMillis,
        first_seen_date     : firstSeenDate,
        ingestion_date      : ingestionDate,
        transformation_date : nowTunisMillis,
        source_system       : sourceSystem,
        is_valid            : (error == null),
        comment             : error,
        partition           : null
    ]

    outputRecords << outputRecord

    if (error) {
        log.warn("Invalid record at index ${idx}: ${error}")
    }
}

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
log.info("Transferred ${outputRecords.size()} total records to success (valid + invalid with consistent schema)")
