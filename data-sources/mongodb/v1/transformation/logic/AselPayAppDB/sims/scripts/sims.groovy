import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.commons.codec.digest.MurmurHash3
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile

// Attributes to inherit
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Read input JSON from FlowFile
def inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON array
def jsonSlurper = new JsonSlurper()
def records
try {
    records = jsonSlurper.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Expected a list of objects but got: ${records.getClass().getName()}")
        session.transfer(session.putAttribute(flowFile, "error", "Expected a list of JSON objects"), REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    session.transfer(session.putAttribute(flowFile, "error", "Invalid JSON: ${e.message}"), REL_FAILURE)
    return
}

def parseDateMillis = { str ->
    try {
        return Instant.parse(str).toEpochMilli()
    } catch (Exception e) {
        return null
    }
}

ZoneId tunisZone = ZoneId.of("Africa/Tunis")
long nowMillis = ZonedDateTime.now(tunisZone).toInstant().toEpochMilli()

List outputRecords = []
List invalidSummary = []

def dbName = flowFile.getAttribute("database_name") ?: "unknown_database"
def collName = flowFile.getAttribute("collection_name") ?: "unknown_collection"

records.each { record ->
    List errors = []

    def id = record._id?.toString()
    def iccid = record.ICCID?.toString()
    def sn = record.SN?.toString()
    def msisdn = record.MSISDN?.toString()
    def imsi = record.IMSI?.toString()
    def mvno = record.mvno?.toString()

    // Validation
    if (!id) errors << "_id missing or invalid"
    if (!iccid) errors << "ICCID missing or invalid"
    if (!sn) errors << "SN missing or invalid"
    if (!msisdn) errors << "MSISDN missing or invalid"
    if (!imsi) errors << "IMSI missing or invalid"
   
    def activationMillis = null
    if (record.activation_date instanceof String)
        activationMillis = parseDateMillis(record.activation_date)
    if (!activationMillis && record.first_seen_date instanceof Number)
        activationMillis = record.first_seen_date as Long
    if (!activationMillis) errors << "activation_date and first_seen_date missing or invalid"

    

    def outputRec = [
        id                  : id ?: null,
        iccid               : iccid ?: null,
        sn                  : sn ?: null,
        msisdn              : msisdn ?: null,
        imsi                : imsi ?: null,
        mvno_id             : mvno ?: null,
        activation_date     : activationMillis ?: null,
        customer_id         : null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : record.source_system ?: null,
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join("; ")
    ]

    outputRecords << outputRec

    if (!errors.isEmpty()) {
        invalidSummary << [
            database_name   : dbName,
            collection_name : collName,
            record_id       : id,
            error_message   : errors.join("; ")
        ]
    }
}

// Write all records to REL_SUCCESS
FlowFile outputFlowFile = session.create(flowFile)
outputFlowFile = session.write(outputFlowFile, { _, os ->
    os.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

outputFlowFile = session.putAttribute(outputFlowFile, 'target_iceberg_table_name', 'sims')
outputFlowFile = session.putAttribute(outputFlowFile, 'schema.name', 'sims')

session.transfer(outputFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} sims records with validation flags")

// Write summary of invalid records to REL_FAILURE if any
if (!invalidSummary.isEmpty()) {
    FlowFile failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { _, os ->
        os.write(JsonOutput.toJson(invalidSummary).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, "error.count", invalidSummary.size().toString())
    failureFlowFile = session.putAttribute(failureFlowFile, "error.type", "validation_summary")
    inheritedAttributes.each { attr ->
        flowFile.getAttribute(attr)?.with {
            failureFlowFile = session.putAttribute(failureFlowFile, attr, it)
        }
    }

    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${invalidSummary.size()} validation errors to REL_FAILURE")
}
