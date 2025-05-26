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

// Transform each record
List transformed = []
records.eachWithIndex { rec, idx ->
    def error = null

    // Validate required fields
    if (!(rec._id instanceof String)) {
        error = "_id must be a string"
    } else if (rec.rejected_At != null) {
        error = "rejected_At is not null (rejected record)"
    } else if (!(rec.sender instanceof String)) {
        error = "sender must be a string"
    } else if (!(rec.recipient instanceof String)) {
        error = "recipient must be a string"
    } else if (!(rec.transactionAmount?.amount instanceof Number)) {
        error = "transactionAmount.amount must be a number"
    }

    // Parse createdAt
    Long createdAt = null
    if (!error) {
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
            error = "createdAt is invalid or missing"
        }
    }

    // Build transformed output
    def out = [
        id                : rec._id ?: null,
        sender_id         : rec.sender ?: null,
        recipient_id      : rec.recipient ?: null,
        transaction_amount : (rec.transactionAmount?.amount as Number)?.toDouble(),
        created_at         : createdAt ?: nowMillis,
        first_seen_date   : rec.first_seen_date ?: null,
        ingestion_date    : rec.ingestion_date ?: null,
        transformation_date: nowMillis,
        source_system     : rec.source_system ?: null,
        is_valid          : (error == null),
        comment           : error
    ]

    // Add partition
    if (createdAt != null) {
        def dt = Instant.ofEpochMilli(createdAt).atZone(tunis)
        out.partition = [dt.getYear(), dt.getMonthValue(), dt.getDayOfMonth()]
    } else {
        out.partition = null
    }

    transformed << out

    if (error) {
        log.warn("Record ${idx} invalid: ${error}")
    }
}

// Write output
FlowFile outFF = session.create(flowFile)
outFF = session.write(outFF, { _, os ->
    os.write(JsonOutput.toJson(transformed).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Copy inherited attrs + metadata
def attrs = [:]
inheritedAttributes.each { k ->
    flowFile.getAttribute(k)?.with { attrs[k] = it }
}
def bytes = JsonOutput.toJson(transformed).getBytes(StandardCharsets.UTF_8)
attrs['file.size'] = bytes.length.toString()
attrs['records.count'] = transformed.size().toString()
attrs['target_iceberg_table_name'] = 'transactions'
attrs['schema.name'] = 'transactions'
attrs.each { k, v -> outFF = session.putAttribute(outFF, k, v) }

// Transfer output
session.transfer(outFF, REL_SUCCESS)
log.info("Transferred ${transformed.size()} records (valid + invalid)")
