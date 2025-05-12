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

// Read raw JSON
String raw = ''
inFF = session.write(inFF, { InputStream is, OutputStream os ->
    raw = IOUtils.toString(is, StandardCharsets.UTF_8)
    os.write(raw.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON array
def parser = new JsonSlurper()
List records
try {
    records = parser.parseText(raw)
    if (!(records instanceof List)) throw new Exception("Expected JSON array of records")
} catch(Exception e) {
    log.error("Failed to parse input JSON", e)
    inFF = session.putAttribute(inFF, 'error', "Invalid JSON: ${e.message}")
    session.transfer(inFF, REL_FAILURE)
    return
}

// Prepare output lists
List valid = []
List invalid = []
long nowMillis = ZonedDateTime.now(ZoneId.of('Africa/Tunis')).toInstant().toEpochMilli()

// Transform each record
records.eachWithIndex { rec, idx ->
    def errs = []
    // Required fields
    if (!rec._id) errs << '_id is required'
    if (rec.first_seen_date == null) errs << 'first_seen_date is required'

    // Determine created_at from first_seen_date
    long createdAt = 0L
    if (rec.first_seen_date != null) {
        try {
            createdAt = rec.first_seen_date instanceof Number ? rec.first_seen_date.longValue()
                         : rec.first_seen_date.toString().toLong()
        } catch(Exception e) {
            errs << "Invalid first_seen_date value: ${rec.first_seen_date}"  
        }
    }

    if (errs) {
        rec._error = errs.join('; ')
        rec._index = idx
        invalid << rec
        log.warn("Record $idx invalid: ${errs.join('; ')}")
        return
    }

    // Build transformed record
    def outRec = [
        _id                 : rec._id,
        name                : rec.name,
        owner               : rec.owner,
        latitude            : rec.latitude,
        longitude           : rec.longitude,
        adresse             : rec.adresse,
        created_at          : createdAt,
        phone               : rec.phone,
        gouvernorat         : rec.gouvernorat,
        status              : rec.status,
        first_seen_date     : rec.first_seen_date,
        ingestion_date      : rec.ingestion_date,
        transformation_date : nowMillis,
        source_system       : rec.source_system
    ]
    valid << outRec
}

// Branch helper
def branch = { List list, Relationship rel, String tableName ->
    if (!list) return
    FlowFile ff = session.create(inFF)
    ff = session.write(ff, { _, os ->
        os.write(JsonOutput.toJson(list).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    def attrs = [:]
    inheritedAttrs.each { k ->
        def v = inFF.getAttribute(k)
        if (v) attrs[k] = v
    }
    attrs['file.size']                 = String.valueOf(JsonOutput.toJson(list).bytes.length)
    attrs['records.count']             = String.valueOf(list.size())
    attrs['target_iceberg_table_name'] = tableName
    attrs.each { k, v -> ff = session.putAttribute(ff, k, v) }
    session.transfer(ff, rel)
}

// Write outputs
branch(valid,   REL_SUCCESS, 'shops')
branch(invalid, REL_FAILURE, 'shoprecord_failed')

// Remove original
session.remove(inFF)
