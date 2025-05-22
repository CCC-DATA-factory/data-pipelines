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
        comment             : errs.isEmpty() ? null : errs.join('; '),
        partition           : null
    ]
    outputRecords << outRec

    if (!errs.isEmpty()) log.warn("Record $idx invalid: ${errs.join('; ')}")
}

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
attrs['file.size'] = jsonBytes.length.toString()
attrs['records.count'] = outputRecords.size().toString()
attrs['target_iceberg_table_name'] = "sallers"
attrs['schema.name'] = "sallers"
attrs.each { k,v -> outFF = session.putAttribute(outFF, k, v) }

session.transfer(outFF, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} total records to success (valid + invalid, with fixed schema)")
