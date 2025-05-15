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

// Read full JSON
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

// Prepare lists
List valid = []
List invalid = []
ZoneId tunisZone = ZoneId.of("Africa/Tunis")
long nowMillis = ZonedDateTime.now(tunisZone).toInstant().toEpochMilli()

// Transformation per record
records.eachWithIndex { rec, idx ->
    def errs = []
    if (!rec._id)      errs << '_id is required'
    if (!rec.SIM)      errs << 'SIM is required'
    if (!rec.customer) errs << 'customer is required'
    if (!rec.code_rio) errs << 'code_rio is required'

    // createdAt must be a Number (epoch‑ms UTC)
    Long origEpoch = (rec.createdAt instanceof Number) ? rec.createdAt as Long : null
    if (origEpoch == null) {
        errs << 'createdAt must be a millisecond timestamp'
    }

    if (errs) {
        rec._error = errs.join('; ')
        rec._index = idx
        invalid << rec
        log.warn("Record $idx invalid: ${errs.join('; ')}")
        return
    }

    // Convert UTC millis to Tunisian‐local millis
    ZonedDateTime utcDt    = Instant.ofEpochMilli(origEpoch).atZone(ZoneOffset.UTC)
    ZoneOffset    offset   = utcDt.getOffset()
    long          localEpoch = origEpoch + offset.getTotalSeconds() * 1000L

    // derive year/month/day from local‐time instant
    ZonedDateTime localDt = Instant.ofEpochMilli(localEpoch).atZone(tunisZone)
    int createdYear  = localDt.getYear()
    int createdMonth = localDt.getMonthValue()
    int createdDay   = localDt.getDayOfMonth()

    // Build transformed record
    def outRec = [
        id                  : rec._id.toString(),
        sim_id              : rec.SIM.toString(),
        created_at          : localEpoch,
        agent_id            : rec.agent ?: '',
        old_number          : rec.old_number ?: '',
        code_rio            : rec.code_rio,
        current_operator    : rec.current_operator ?: '',
        customer_id         : rec.customer.toString(),
        mvno_id             : rec.mvno_id ? rec.mvno_id.toString() : '',
        first_seen_date     : rec.first_seen_date,
        ingestion_date      : rec.ingestion_date,
        transformation_date : nowMillis,
        source_system       : rec.source_system,
        partition           : [ createdYear, createdMonth, createdDay ]
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
    attrs['file.size']             = String.valueOf(JsonOutput.toJson(list).bytes.length)
    attrs['records.count']         = String.valueOf(list.size())
    attrs['target_iceberg_table_name'] = tableName
    attrs.each { k,v -> ff = session.putAttribute(ff, k, v) }
    session.transfer(ff, rel)
}

// Write valid and invalid
branch(valid,   REL_SUCCESS, 'portability')
branch(invalid, REL_FAILURE, 'portability_failed')

// Remove original
session.remove(inFF)
