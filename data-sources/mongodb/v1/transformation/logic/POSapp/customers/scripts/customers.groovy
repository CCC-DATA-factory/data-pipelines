import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import java.time.*
import org.apache.commons.codec.digest.MurmurHash3
import org.apache.nifi.flowfile.FlowFile

def session = session
def log = log

FlowFile inputFlowFile = session.get()
if (!inputFlowFile) return

def inputJson = ''
inputFlowFile = session.write(inputFlowFile, { inputStream, outputStream ->
    inputJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(inputJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def jsonSlurper = new JsonSlurper()
def records
try {
    records = jsonSlurper.parseText(inputJson)
    if (!(records instanceof List)) {
        log.error("Expected a list of objects but got: ${records.getClass().getName()}")
        inputFlowFile = session.putAttribute(inputFlowFile, "error", "Input is not a list")
        session.transfer(inputFlowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    inputFlowFile = session.putAttribute(inputFlowFile, "error", "Invalid JSON: ${e.message}")
    session.transfer(inputFlowFile, REL_FAILURE)
    return
}

ZoneId tunisZone = ZoneId.of("Africa/Tunis")
long nowMillis = ZonedDateTime.now(tunisZone).toInstant().toEpochMilli()

List finalRecords = []

records.eachWithIndex { record, idx ->
    def error = null

    // Validation
    if (!record._id || !record.cin || record.mvno_id == null) {
        error = "Missing required fields (_id, cin, mvno_id)"
        log.warn("Invalid record at index ${idx}: ${error}")
    }

    // Compute creationMillis
    def creationMillis = null
    if (record.creation_date != null && record.creation_date.toString().isLong()) {
        def utcMillis = record.creation_date as Long
        creationMillis = ZonedDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), ZoneOffset.UTC)
                                .withZoneSameInstant(tunisZone)
                                .toInstant().toEpochMilli()
    } else if (record.first_seen_date != null) {
        creationMillis = record.first_seen_date as Long
    }

    // Compute bucket = hash(_id) mod 12
    int bucket = 0
    if (record._id) {
        byte[] idBytes = (record._id as String).getBytes(StandardCharsets.UTF_8)
        int rawHash = MurmurHash3.hash32(idBytes, 0, idBytes.length, 0)
        bucket = Math.abs(rawHash) % 12
    }

    def transformed = [
        _id                 : record._id ?: null,
        DOB                 : record.DOB ?: null,
        POB                 : record.POB ?: null,
        address             : record.address ?: null,
        arta_id             : record.arta_id ?: null,
        cin                 : record.cin ?: null,
        cin_recto_path      : record.cin_recto_path ?: null,
        cin_verso_path      : record.cin_verso_path ?: null,
        city                : record.city ?: null,
        creation_date       : creationMillis,
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
        partition           : [ bucket ],
        is_valid            : (error == null),
        comment             : error
    ]

    finalRecords << transformed
}

if (!finalRecords.isEmpty()) {
    def outputFlowFile = session.create(inputFlowFile)
    outputFlowFile = session.write(outputFlowFile, { out ->
        out.write(JsonOutput.toJson(finalRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    outputFlowFile = session.putAttribute(outputFlowFile, "target_iceberg_table_name", "cutomers")
    outputFlowFile = session.putAttribute(outputFlowFile, "schema.name", "cutomers")
    outputFlowFile = session.putAttribute(outputFlowFile, "record.count", finalRecords.size().toString())

    session.transfer(outputFlowFile, REL_SUCCESS)
    log.info("Transferred ${finalRecords.size()} total records (valid + invalid) to success with flags")
}

session.remove(inputFlowFile)
