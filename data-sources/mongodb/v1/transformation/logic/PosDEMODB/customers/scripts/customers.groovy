import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import java.time.*
import org.apache.commons.codec.digest.MurmurHash3
import org.apache.nifi.flowfile.FlowFile

// Get database_name and collection_name attributes for failure records
def database_name = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collection_name = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

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

List finalRecords = []
List failureRecords = []


def parseLongSafe = { obj ->
    try {
        return obj?.toString()?.toLong()
    } catch (_) {
        return null
    }
}
long utcToTunisLocalEpochMillis(long utcEpochMillis) {
    ZoneId tunisZone = ZoneId.of("Africa/Tunis")

    // Convert UTC millis to Instant
    Instant instant = Instant.ofEpochMilli(utcEpochMillis)

    // Get the ZonedDateTime in Tunisia timezone for that instant
    ZonedDateTime tunisZoned = instant.atZone(tunisZone)

    // Get local date/time components
    LocalDateTime tunisLocalDateTime = tunisZoned.toLocalDateTime()

    // Now interpret that local date/time as if it were UTC, get the epoch millis
    long shiftedMillis = tunisLocalDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli()

    return shiftedMillis
}

records.eachWithIndex { record, idx ->
    def error = null

    // Validation
    if (!record._id || !record.cin ) {
        error = "Missing required fields (_id, cin)"
        log.warn("Invalid record at index ${idx}: ${error}")

        // Add to failure list with requested fields
        failureRecords << [
            database_name   : database_name,
            collection_name : collection_name,
            record_id       : record._id ?: null,
            error_message   : error
        ]
    }

    // Compute creationMillis
    def creationMillis = null

    if (record.creation_date != null && parseLongSafe(record.creation_date) != null) {
        def utcMillis = parseLongSafe(record.creation_date)
        creationMillis = utcToTunisLocalEpochMillis(utcMillis)
    } else if (record.first_seen_date != null) {
        creationMillis = parseLongSafe(record.first_seen_date)
    }




    def transformed = [
        id                 : record._id ?: null,
        dob                 : record.DOB ?: null,
        pob                 : record.POB ?: null,
        address             : record.address ?: null,
        arta_id             : record.arta_id ?: null,
        cin                 : record.cin ?: null,
        cin_recto_path      : record.cin_recto_path ?: null,
        cin_verso_path      : record.cin_verso_path ?: null,
        city                : record.city ?: null,
        creation_date       : creationMillis ?: null,
        email               : record.email ?: null,
        first_name          : record.first_name ?: null,
        gender              : record.gender ?: null,
        issue_date          : record.issue_date ?: null,
        job                 : record.job ?: null,
        last_name           : record.last_name ?: null,
        mvno_id             : record.mvno_id ?: null,
        passport            : record.passport ?: null,
        passport_path       : record.passport_path ?: null,
        postal_code         : record.postal_code ?: null,
        region              : record.region ?: null,
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : record.source_system ?: null,
        is_valid            : (error == null),
        comment             : error
    ]

    finalRecords << transformed
}

if (!finalRecords.isEmpty()) {
    def outputFlowFile = session.create(flowFile)
    outputFlowFile = session.write(outputFlowFile, { out ->
        out.write(JsonOutput.toJson(finalRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    outputFlowFile = session.putAttribute(outputFlowFile, "target_iceberg_table_name", "customers")
    outputFlowFile = session.putAttribute(outputFlowFile, "schema.name", "customers")

    session.transfer(outputFlowFile, REL_SUCCESS)
    log.info("Transferred ${finalRecords.size()} total records (valid + invalid) to success with flags")
}

// Transfer failure records if any
if (!failureRecords.isEmpty()) {
    def failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { out ->
        out.write(JsonOutput.toJson(failureRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', "${failureRecords.size()}")
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type', 'validation_summary')
    failureFlowFile = session.putAttribute(failureFlowFile, "database_name", database_name)
    failureFlowFile = session.putAttribute(failureFlowFile, "collection_name", collection_name)

    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${failureRecords.size()} validation errors to FAILURE")
}
