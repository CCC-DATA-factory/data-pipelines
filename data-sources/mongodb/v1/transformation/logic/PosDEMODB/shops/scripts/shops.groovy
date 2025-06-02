import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.flowfile.FlowFile

def inheritedAttrs = ['filepath', 'database_name', 'collection_name']

// Read raw JSON safely
String raw = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    raw = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(raw.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON array safely
def parser = new JsonSlurper()
List records
try {
    records = parser.parseText(raw)
    if (!(records instanceof List)) {
        throw new Exception("Expected JSON array of records")
    }
} catch(Exception e) {
    log.error("Failed to parse input JSON", e)
    flowFile = session.putAttribute(flowFile, 'error', "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

// Get inherited attributes
def database_name = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collection_name = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

// Prepare unified output list and failure error list
List outputRecords = []
List failureRecords = []

long nowMillis = ZonedDateTime.now(ZoneId.of('Africa/Tunis')).toInstant().toEpochMilli()

// Transform each record
records.eachWithIndex { rec, idx ->
    def errs = []

    if (!rec._id) errs << '_id is required'
    if (!rec.name) errs << 'name is required'
    if (rec.first_seen_date == null) errs << 'first_seen_date is required'

    long createdAt = 0L
    if (rec.first_seen_date != null) {
        try {
            createdAt = rec.first_seen_date instanceof Number
                ? rec.first_seen_date.longValue()
                : rec.first_seen_date.toString().toLong()
        } catch(Exception e) {
            errs << "Invalid first_seen_date value: ${rec.first_seen_date}"
        }
    }

    def outRec = [
        id                  : rec._id ?: null,
        name                : rec.name ?: null,
        owner               : rec.owner ?: null,
        reseller_id         : rec.reseller_id ?: null,
        latitude            : rec.latitude ?: null,
        longitude           : rec.longitude ?: null,
        adresse             : rec.adresse ?: null,
        created_at          : createdAt,
        phone               : rec.phone ?: null,
        gouvernorat         : rec.gouvernorat ?: null,
        status              : rec.status ?: null,
        first_seen_date     : rec.first_seen_date ?: null,
        ingestion_date      : rec.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : rec.source_system ?: null,
        is_valid            : errs.isEmpty(),
        comment             : errs ? errs.join('; ') : null
    ]

    outputRecords << outRec

    if (!errs.isEmpty()) {
        failureRecords << [
            database_name   : database_name,
            collection_name : collection_name,
            record_id       : rec._id ?: null,
            error_message   : errs.join('; ')
        ]
        log.warn("Record ${idx} invalid: ${errs.join('; ')}")
    }
}

// Create new FlowFile with transformed data (both valid + invalid)
FlowFile outFF = session.create(flowFile)
outFF = session.write(outFF, { outputStream ->
    outputStream.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as OutputStreamCallback)

// Add output attributes
def attrs = [:]
inheritedAttrs.each { k ->
    def v = flowFile.getAttribute(k)
    if (v) attrs[k] = v
}

attrs['target_iceberg_table_name'] = 'shops'
attrs['schema.name'] = "shops"

attrs.each { k, v -> outFF = session.putAttribute(outFF, k, v) }

// Transfer to SUCCESS
session.transfer(outFF, REL_SUCCESS)

// Generate validation error summary (if any) to REL_FAILURE
if (!failureRecords.isEmpty()) {
    FlowFile errorFF = session.create(flowFile)
    errorFF = session.write(errorFF, { out ->
        out.write(JsonOutput.toJson(failureRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    errorFF = session.putAttribute(errorFF, 'error.count', "${failureRecords.size()}")
    errorFF = session.putAttribute(errorFF, 'error.type', 'validation_summary')

    inheritedAttrs.each { attr ->
        flowFile.getAttribute(attr)?.with { errorFF = session.putAttribute(errorFF, attr, it) }
    }

    session.transfer(errorFF, REL_FAILURE)
    log.info("Transferred ${failureRecords.size()} validation error summaries to FAILURE")
}
