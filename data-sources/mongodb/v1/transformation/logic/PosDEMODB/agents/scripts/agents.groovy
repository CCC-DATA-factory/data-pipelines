import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.flowfile.FlowFile

def inheritedAttrs = ['filepath', 'database_name', 'collection_name']

// Read input JSON safely
String rawJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    rawJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(rawJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON array
List records
try {
    records = new JsonSlurper().parseText(rawJson)
    if (!(records instanceof List)) {
        throw new Exception("Expected JSON array of records")
    }
} catch(Exception e) {
    log.error("Failed to parse input JSON", e)
    flowFile = session.putAttribute(flowFile, 'error', "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()
List outputRecords = []
List failureRecords = []

// Read database_name and collection_name from attributes once
def database_name = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collection_name = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

records.eachWithIndex { rec, idx ->
    def errs = []
    def id = rec._id?.toString() ?: null
    def firstName = rec.first_name?.toString()?.trim() ?: null
    def lastName = rec.last_name?.toString()?.trim() ?: null
    def name = (firstName ?: '') + ' ' + (lastName ?: '')
    def parentId = rec.reseller?.toString() ?: null
    def shopName = null
    def joinedAt = null
    def joinedRaw = rec.created_at ?: rec.first_seen_date

    if (id == null) errs << '_id is required'
    if (firstName == null) errs << 'first_name is required'
    if (lastName == null) errs << 'last_name is required'

    if (joinedRaw != null) {
        try {
            joinedAt = joinedRaw instanceof Number ? joinedRaw.longValue() : joinedRaw.toString().toLong()
        } catch(Exception e) {
            errs << "Invalid timestamp value: ${joinedRaw}"
        }
    } else {
        errs << 'Either created_at or first_seen_date must be present'
    }

    def outRec = [
        id                  : id,
        name                : name,
        role                : 'agent',
        parent_id           : parentId,
        shop_name           : shopName,
        joined_at           : joinedAt,
        first_seen_date     : rec.first_seen_date ?: null,
        ingestion_date      : rec.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : rec.source_system ?: null,
        is_valid            : errs.isEmpty(),
        comment             : errs.isEmpty() ? null : errs.join('; ')
    ]
    outputRecords << outRec

    if (!errs.isEmpty()) {
        failureRecords << [
            database_name   : database_name,
            collection_name : collection_name,
            record_id       : id,
            error_message   : errs.join('; ')
        ]
        log.warn("Record $idx invalid: ${errs.join('; ')}")
    }
}

// Write all (valid + invalid) to success FlowFile
FlowFile outFF = session.create(flowFile)
outFF = session.write(outFF, { out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as OutputStreamCallback)

// Copy attributes and attach metadata
def attrs = [:]
inheritedAttrs.each { k ->
    def v = flowFile.getAttribute(k)
    if (v != null) attrs[k] = v
}
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
attrs['target_iceberg_table_name'] = "sallers"
attrs['schema.name'] = "sallers"
attrs.each { k,v -> outFF = session.putAttribute(outFF, k, v) }

session.transfer(outFF, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} total records to success (valid + invalid, with fixed schema)")

// If failure records exist, send them as separate FlowFile to failure
if (!failureRecords.isEmpty()) {
    FlowFile failureFF = session.create(flowFile)
    failureFF = session.write(failureFF, { out ->
        out.write(JsonOutput.toJson(failureRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    failureFF = session.putAttribute(failureFF, 'error.count', failureRecords.size().toString())
    failureFF = session.putAttribute(failureFF, 'error.type', 'validation_summary')
    // Copy inherited attributes
    inheritedAttrs.each { k ->
        def v = flowFile.getAttribute(k)
        if (v != null) failureFF = session.putAttribute(failureFF, k, v)
    }

    session.transfer(failureFF, REL_FAILURE)
    log.info("Transferred ${failureRecords.size()} validation errors to failure")
}
