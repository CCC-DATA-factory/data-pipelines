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
        log.error("Input is not a JSON array")
        flowFile = session.putAttribute(flowFile, "error", "Expected JSON array")
        session.transfer(flowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse JSON", e)
    flowFile = session.putAttribute(flowFile, "error", "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

// Compute now in Tunis timezone
ZoneId tunis = ZoneId.of("Africa/Tunis")
long nowMillis = ZonedDateTime.now(tunis).toInstant().toEpochMilli()
def dbName = flowFile.getAttribute("database_name") ?: "unknown_database"
def collName = flowFile.getAttribute("collection_name") ?: "unknown_collection"

// Transform each record and collect invalid summary
List transformed = []
List invalidSummary = []

records.eachWithIndex { rec, idx ->

    List errors = []

    // Validate required fields
    if (!(rec._id instanceof String)) {
        errors << "_id must be a string"
    }
    if (rec.rejected_At != null) {
        errors << "rejected_At is not null (rejected record)"
    }
    if (!(rec.sender instanceof String)) {
        errors << "sender must be a string"
    }
    if (!(rec.recipient instanceof String)) {
        errors << "recipient must be a string"
    }
    if (!(rec.transactionAmount?.amount instanceof Number)) {
        errors << "transactionAmount.amount must be a number"
    }

    // Parse createdAt
    Long createdAt = null
    if (errors.isEmpty()) {
        if (rec.createdAt instanceof Number) {
            createdAt = (rec.createdAt as Number).longValue()
        } else if (rec.createdAt instanceof String) {
            try {
                createdAt = Instant.parse(rec.createdAt).toEpochMilli()
            } catch (_) {
                createdAt = null
            }
        }
        if (createdAt == null && rec.first_seen_date instanceof Number) {
            createdAt = (rec.first_seen_date as Number).longValue()
        }
        if (createdAt == null) {
            errors << "createdAt is invalid or missing"
        }
    }

    // Build transformed output
    def out = [
        id                 : rec._id ?: null,
        sender_id          : rec.sender ?: null,
        recipient_id       : rec.recipient ?: null,
        transaction_amount : (rec.transactionAmount?.amount as Number)?.toDouble(),
        created_at         : createdAt ?: nowMillis,
        first_seen_date    : rec.first_seen_date ?: null,
        ingestion_date     : rec.ingestion_date ?: null,
        transformation_date: nowMillis,
        source_system      : rec.source_system ?: null,
        is_valid           : errors.isEmpty(),
        comment            : errors.isEmpty() ? null : errors.join("; ")
    ]

    transformed << out

    if (!errors.isEmpty()) {
        log.warn("Record ${idx} invalid: ${errors.join('; ')}")
        invalidSummary << [
            database_name   : dbName,
            collection_name : collName,
            record_id       : rec._id ?: null,
            error_message   : errors.join('; ')
        ]
    }
}

// --- Write all records (valid + invalid) to REL_SUCCESS ---
FlowFile successFF = session.create(flowFile)
successFF = session.write(successFF, { _, os ->
    os.write(JsonOutput.toJson(transformed).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Copy inherited attributes + metadata
def successAttrs = [:]
inheritedAttributes.each { k ->
    flowFile.getAttribute(k)?.with { successAttrs[k] = it }
}
def successBytes = JsonOutput.toJson(transformed).getBytes(StandardCharsets.UTF_8)
successAttrs['target_iceberg_table_name'] = 'transactions'
successAttrs['schema.name'] = 'transactions'
successAttrs.each { k, v -> successFF = session.putAttribute(successFF, k, v) }

session.transfer(successFF, REL_SUCCESS)
log.info("Transferred ${transformed.size()} records (valid + invalid) to REL_SUCCESS")

// --- Write invalid summary to REL_FAILURE only if there are invalid records ---
if (!invalidSummary.isEmpty()) {
    FlowFile failureFF = session.create(flowFile)
    failureFF = session.write(failureFF, { _, os ->
        os.write(JsonOutput.toJson(invalidSummary).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

    // Copy inherited attributes to failure FF as well
    def failureAttrs = [:]
    inheritedAttributes.each { k ->
        flowFile.getAttribute(k)?.with { failureAttrs[k] = it }
    }
    def failureBytes = JsonOutput.toJson(invalidSummary).getBytes(StandardCharsets.UTF_8)

    failureAttrs['error.type'] = "validation_summary"
    failureAttrs['error.count'] = invalidSummary.size().toString()
    failureAttrs.each { k, v -> failureFF = session.putAttribute(failureFF, k, v) }

    session.transfer(failureFF, REL_FAILURE)
    log.info("Transferred ${invalidSummary.size()} invalid records summary to REL_FAILURE")
}
