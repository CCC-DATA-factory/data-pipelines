import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.Relationship

// Setup
def log = log
def session = session
def inheritedAttrs = ['filepath', 'database_name', 'collection_name']

// Fetch incoming FlowFile
FlowFile inFF = session.get()
if (!inFF) return

// Read full JSON payload
def rawJson = ''
inFF = session.write(inFF, { InputStream is, OutputStream os ->
    rawJson = IOUtils.toString(is, StandardCharsets.UTF_8)
    os.write(rawJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON array
def parser = new JsonSlurper()
List records
try {
    records = parser.parseText(rawJson)
    if (!(records instanceof List)) {
        throw new Exception("Expected JSON array of records")
    }
} catch(Exception e) {
    log.error("Failed to parse input JSON", e)
    inFF = session.putAttribute(inFF, 'error', "Invalid JSON: ${e.message}")
    session.transfer(inFF, REL_FAILURE)
    return
}

// Prepare lists for valid and invalid
List valid = []
List invalid = []
long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

records.eachWithIndex { rec, idx ->
    def errs = []
    // Required source fields
    if (!rec._id)                errs << '_id is required'
    if (!rec.first_name)         errs << 'first_name is required'
    if (!rec.last_name)          errs << 'last_name is required'
    
    // Determine join timestamp: created_at or first_seen_date
    long joinedAt = 0L
    if (rec.created_at != null) {
        try {
            joinedAt = rec.created_at instanceof Number ? rec.created_at.longValue() : rec.created_at.toString().toLong()
        } catch(Exception e) {
            errs << "Invalid created_at value: ${rec.created_at}"
        }
    } else if (rec.first_seen_date != null) {
        joinedAt = rec.first_seen_date instanceof Number ? rec.first_seen_date.longValue() : rec.first_seen_date.toString().toLong()
    } else {
        errs << 'Either created_at or first_seen_date must be present'
    }

    if (errs) {
        rec._error = errs.join('; ')
        rec._index = idx
        invalid << rec
        log.warn("Record $idx invalid: ${errs.join('; ')}")
        return
    }

    // Build output record
    def outRec = [
        id                  : rec._id.toString(),
        name                : rec.first_name.toString().trim() + ' ' + rec.last_name.toString().trim(),
        role                : 'agent',
        parent_id           : rec.reseller != null ? rec.reseller.toString() : null,
        joined_at           : joinedAt,
        first_seen_date     : rec.first_seen_date,
        ingestion_date      : rec.ingestion_date,
        transformation_date : nowMillis,
        source_system       : rec.source_system
    ]
    valid << outRec
}

// Helper to branch lists into new FlowFiles

def branch = { List list, Relationship rel, String tableName ->
    if (!list) return
    FlowFile outFF = session.create(inFF)
    outFF = session.write(outFF, { _, os ->
        os.write(JsonOutput.toJson(list).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    def attrs = [:]
    inheritedAttrs.each { k ->
        def v = inFF.getAttribute(k)
        if (v) attrs[k] = v
    }
    attrs['file.size']               = String.valueOf(JsonOutput.toJson(list).bytes.length)
    attrs['records.count']           = String.valueOf(list.size())
    attrs['target_iceberg_table_name'] = tableName
    attrs.each { k,v -> outFF = session.putAttribute(outFF, k, v) }
    session.transfer(outFF, rel)
}

// Write valid to success, invalid to failure
branch(valid,   REL_SUCCESS, 'saller')
branch(invalid, REL_FAILURE, 'saller_failed')

// Remove original FlowFile
session.remove(inFF)
