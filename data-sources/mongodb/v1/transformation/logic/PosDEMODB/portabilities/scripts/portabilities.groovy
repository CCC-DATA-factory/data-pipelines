import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import java.time.*
import org.apache.nifi.flowfile.FlowFile

// Inherit these attributes to new flowfiles
def inheritedAttributes = ['database_name', 'collection_name']

// Get database_name and collection_name from flowFile attributes or default
def database_name = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collection_name = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

// Read FlowFile content into a String
def inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON input
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
List failureRecords = []

records.each { record ->
    def errorMessages = []
    def createdMillis = null

    // Handle createdAt with fallback to first_seen_date
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

    // Required fields validations
    if (!record._id) errorMessages << "_id missing"
    if (!record.SIM) errorMessages << "SIM missing"
    if (!record.customer) errorMessages << "customer missing"
    if (!record.code_rio) errorMessages << "code_rio missing"

    // Build transformed record
    def dt = createdMillis ? Instant.ofEpochMilli(createdMillis).atZone(tunisZone) : null
    def outputRec = [
        id                  : record._id?.toString() ?: null,
        sim_id              : record.SIM?.toString() ?: null,
        created_at          : createdMillis ?: null,
        agent_id            : record.agent ?: null,
        old_number          : record.old_number ?: null,
        code_rio            : record.code_rio ?: null,
        current_operator    : record.current_operator ?: null,
        customer_id         : record.customer?.toString() ?: null,
        mvno_id             : record.mvno_id?.toString() ?: null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : record.source_system ?: null,
        is_valid            : errorMessages.isEmpty(),
        comment             : errorMessages ? errorMessages.join("; ") : null
    ]

    outputRecords << outputRec

    // Prepare failure record for invalid data only
    if (!errorMessages.isEmpty()) {
        failureRecords << [
            database_name   : database_name,
            collection_name : collection_name,
            record_id       : record._id?.toString() ?: null,
            error_message   : errorMessages.join("; ")
        ]
    }
}

// Write all records (valid and invalid) to success
FlowFile successFlowFile = session.create(flowFile)
successFlowFile = session.write(successFlowFile, { _, out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "portability")
successFlowFile = session.putAttribute(successFlowFile, "schema.name", "portability_in")
successFlowFile = session.putAttribute(successFlowFile, "record.count", outputRecords.size().toString())

session.transfer(successFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} portability records with validation flags")

// If failure records exist, create a separate flowfile and send to failure relationship
if (!failureRecords.isEmpty()) {
    FlowFile failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { _, out ->
        out.write(JsonOutput.toJson(failureRecords).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', "${failureRecords.size()}")
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type', 'validation_summary')

    // Copy inherited attributes
    inheritedAttributes.each { attr ->
        def val = flowFile.getAttribute(attr)
        if (val != null) failureFlowFile = session.putAttribute(failureFlowFile, attr, val)
    }

    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${failureRecords.size()} validation errors to FAILURE")
}
