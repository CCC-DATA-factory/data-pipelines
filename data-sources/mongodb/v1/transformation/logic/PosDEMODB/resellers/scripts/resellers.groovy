import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.flowfile.FlowFile


def inheritedAttrs = ['filepath', 'database_name', 'collection_name']


// Read JSON string from input stream
String raw = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    raw = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(raw.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def parser = new JsonSlurper()
List records
try {
    records = parser.parseText(raw)
    if (!(records instanceof List)) throw new Exception("Expected JSON array of records")
} catch(Exception e) {
    log.error("Failed to parse input JSON", e)
    flowFile = session.putAttribute(flowFile, 'error', "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

long nowMillis = ZonedDateTime.now(ZoneId.of('Africa/Tunis')).toInstant().toEpochMilli()
List outputRecords = []

records.eachWithIndex { rec, idx ->
    def errs = []

    def id = rec._id?.toString()
    def name = rec.name?.toString()
    def firstSeen = rec.first_seen_date
    def ingestionDate = rec.ingestion_date
    def sourceSystem = rec.source_system

    if (!id) errs << '_id is required'
    if (!name) errs << 'name is required'
    if (firstSeen == null) errs << 'first_seen_date is required'

    long joinedAt = 0L
    if (firstSeen != null) {
        try {
            joinedAt = firstSeen instanceof Number ? firstSeen.longValue() : firstSeen.toString().toLong()
        } catch (Exception e) {
            errs << "Invalid first_seen_date value: ${firstSeen}"
        }
    }

    def outRec = [
        id                  : id ?: null,
        name                : name ?: null,
        role                : 'reseller',
        parent_id           : null,
        shop_name           : null,
        joined_at           : joinedAt,
        first_seen_date     : firstSeen ?: null,
        ingestion_date      : ingestionDate ?: null,
        transformation_date : nowMillis,
        source_system       : sourceSystem ?: null,
        is_valid            : errs.isEmpty(),
        comment             : errs ? errs.join('; ') : null,
        partition           : null
    ]

    outputRecords << outRec
    if (errs) log.warn("Record $idx invalid: ${errs.join('; ')}")
}

// Write combined output
FlowFile outFF = session.create(flowFile)
outFF = session.write(outFF, { outputStream ->
    outputStream.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as OutputStreamCallback)

// Add attributes
def attrs = [:]
inheritedAttrs.each { k ->
    def v = flowFile.getAttribute(k)
    if (v) attrs[k] = v
}
attrs['file.size'] = String.valueOf(JsonOutput.toJson(outputRecords).bytes.length)
attrs['records.count'] = String.valueOf(outputRecords.size())
attrs['target_iceberg_table_name'] = "sallers"
attrs['schema.name'] = "sallers"

attrs.each { k, v -> outFF = session.putAttribute(outFF, k, v) }

session.transfer(outFF, REL_SUCCESS)
