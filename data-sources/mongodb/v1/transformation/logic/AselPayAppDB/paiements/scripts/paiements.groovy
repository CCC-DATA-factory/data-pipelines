import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import java.time.*
import org.apache.nifi.flowfile.FlowFile

def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

def inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def jsonSlurper = new JsonSlurper()
def records
try {
    records = jsonSlurper.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Expected a list of objects but got: " + records.getClass().getName())
        session.transfer(flowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    session.transfer(flowFile, REL_FAILURE)
    return
}

ZoneId tunisZone = ZoneId.of("Africa/Tunis")
long nowMillis = ZonedDateTime.now(tunisZone).toInstant().toEpochMilli()

List outputRecords = []
List invalidRecordsSummary = []

def databaseName = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collectionName = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

records.each { record ->
    def errorMessages = []
    def createdMillis = null

    if (record.createdAt instanceof Number) {
        def utcMillis = record.createdAt as Long
        createdMillis = ZonedDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), ZoneOffset.UTC)
                            .withZoneSameInstant(tunisZone)
                            .toInstant().toEpochMilli()
    } else if (record.first_seen_date instanceof Number) {
        createdMillis = record.first_seen_date as Long
    } else {
        errorMessages << "Missing or invalid createdAt/first_seen_date"
    }

    def outputRec = [
        id                  : record._id ?: null,
        commercial_id       : record.commercial ?: null,
        franchise_id        : record.franchise ?: null,
        transaction_id      : record.transaction ?: null,
        amount              : record.amount != null ? (record.amount instanceof Number ? record.amount.toDouble() : record.amount.toString().toDouble()) : null,
        created_at          : createdMillis,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : record.source_system ?: null
    ]

    if (!record._id)        errorMessages << "_id missing"
    if (!record.commercial) errorMessages << "commercial missing"
    if (!record.franchise)  errorMessages << "franchise missing"
    if (!record.transaction) errorMessages << "transaction missing"
    if (record.amount == null) errorMessages << "amount missing"

    outputRec['is_valid'] = errorMessages.isEmpty()
    outputRec['comment'] = errorMessages.isEmpty() ? null : errorMessages.join("; ")
    outputRecords << outputRec

    if (!errorMessages.isEmpty()) {
        invalidRecordsSummary << [
            database_name   : databaseName,
            collection_name : collectionName,
            record_id       : record._id?.toString() ?: null,
            error_message   : errorMessages.join("; ")
        ]
    }
}

// Write all records (valid + invalid) to success
FlowFile successFlowFile = session.create(flowFile)
successFlowFile = session.write(successFlowFile, { _ , out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "paiement")
successFlowFile = session.putAttribute(successFlowFile, "schema.name", "paiement")
successFlowFile = session.putAttribute(successFlowFile, "record.count", outputRecords.size().toString())

inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { successFlowFile = session.putAttribute(successFlowFile, attr, it) }
}

session.transfer(successFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records with validation flags")

// If validation errors exist, transfer to REL_FAILURE
if (!invalidRecordsSummary.isEmpty()) {
    FlowFile failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { _, out ->
        out.write(JsonOutput.toJson(invalidRecordsSummary).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', invalidRecordsSummary.size().toString() )
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type', 'validation_summary')
    inheritedAttributes.each { k -> flowFile.getAttribute(k)?.with { failureFlowFile = session.putAttribute(failureFlowFile, k, it) } }
    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${invalidRecordsSummary.size()} validation errors to REL_FAILURE")
}
