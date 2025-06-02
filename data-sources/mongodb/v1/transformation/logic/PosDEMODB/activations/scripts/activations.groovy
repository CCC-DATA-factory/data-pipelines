import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import java.time.*

// Get database_name and collection_name from attributes, fallback to unknown
def database_name = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collection_name = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

// Safely read input using 2-arg StreamCallback
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
        log.error("Expected a list of objects but got: ${records.getClass().getName()}")
        flowFile = session.putAttribute(flowFile, "error", "Input is not a list")
        session.transfer(flowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    flowFile = session.putAttribute(flowFile, "error", "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

ZoneId tunisZone = ZoneId.of("Africa/Tunis")
long nowMillis = ZonedDateTime.now(tunisZone).toInstant().toEpochMilli()

List outputRecords = []
List failureRecords = []  // <-- list for errors to send to FAILURE

records.eachWithIndex { record, idx ->
    def createdAtMillis = null
    def errorMessages = []

    // Rule 1: Must be completed
    if (!record.completed) {
        errorMessages << "Not completed"
    }

    // Rule 2: Validate creation_date
    if (record.creation_date != null && record.creation_date.toString().isLong()) {
        def utcMillis = record.creation_date as Long
        createdAtMillis = ZonedDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), ZoneOffset.UTC)
            .withZoneSameInstant(tunisZone)
            .toInstant().toEpochMilli()
    } else if (record.first_seen_date != null) {
        createdAtMillis = record.first_seen_date as Long
    } else {
        errorMessages << "Missing creation_date or first_seen_date"
    }

    // Rule 3: Required fields
    if (!record._id) errorMessages << "_id is required"
    if (!record.SIM) errorMessages << "SIM is required"
    if (!record.agent) errorMessages << "agent is required"
    if (!record.customer) errorMessages << "customer is required"
    if (createdAtMillis == null) errorMessages << "Invalid createdAtMillis"

    def createdYear = null, createdMonth = null, createdDay = null
    if (createdAtMillis != null) {
        def dt = Instant.ofEpochMilli(createdAtMillis).atZone(tunisZone)
        createdYear = dt.getYear()
        createdMonth = dt.getMonthValue()
        createdDay = dt.getDayOfMonth()
    }

    def shop = (record.shopName ?: "Unknown").toString()

    def transformed = [
        id                  : record._id ?: null,
        sim_id              : record.SIM ?: null,
        agent_id            : record.agent ?: null,
        customer_id         : record.customer ?: null,
        mvno_id             : record.mvno_id?.toString(),
        created_at          : createdAtMillis,
        shop_name           : shop,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : record.source_system ?: null,
        is_valid            : errorMessages.isEmpty(),
        comment             : errorMessages ? errorMessages.join("; ") : null
    ]

    outputRecords << transformed

    // If errors, add to failureRecords list
    if (!errorMessages.isEmpty()) {
        failureRecords << [
            database_name   : database_name,
            collection_name : collection_name,
            record_id       : record._id ?: null,
            error_message   : errorMessages.join("; ")
        ]
    }
}

// Write output safely using OutputStreamCallback (1-arg)
if (!outputRecords.isEmpty()) {
    def successFlowFile = session.create(flowFile)
    successFlowFile = session.write(successFlowFile, { out ->
        out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "activation")
    successFlowFile = session.putAttribute(successFlowFile, "schema.name", "activation")
    successFlowFile = session.putAttribute(successFlowFile, "record.count", outputRecords.size().toString())

    session.transfer(successFlowFile, REL_SUCCESS)
    log.info("Transferred ${outputRecords.size()} activation records with validation metadata")
}

// Write failure records if any to separate FlowFile sent to REL_FAILURE
if (!failureRecords.isEmpty()) {
    def failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { out ->
        out.write(JsonOutput.toJson(failureRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', "${failureRecords.size()}")
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type', 'validation_summary')

    // Inherit attributes for failure FlowFile
    ['database_name', 'collection_name', 'filepath'].each { attr ->
        flowFile.getAttribute(attr)?.with { failureFlowFile = session.putAttribute(failureFlowFile, attr, it) }
    }

    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${failureRecords.size()} validation errors to FAILURE")
}
