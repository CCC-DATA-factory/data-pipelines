import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.commons.codec.digest.MurmurHash3
import org.apache.nifi.processor.io.StreamCallback
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
        session.transfer(inputFlowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    session.transfer(inputFlowFile, REL_FAILURE)
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

records.each { record ->
    List errors = []

    def id = record._id?.toString()
    def iccid = record.ICCID?.toString()
    def sn = record.SN?.toString()
    def msisdn = record.MSISDN?.toString()
    def imsi = record.IMSI?.toString()
    def mvno = record.mvno?.toString()

    // Validate required fields
    if (!id) errors << "_id missing or invalid"
    if (!iccid) errors << "ICCID missing or invalid"
    if (!sn) errors << "SN missing or invalid"
    if (!msisdn) errors << "MSISDN missing or invalid"
    if (!imsi) errors << "IMSI missing or invalid"
    if (!mvno) errors << "mvno missing or invalid"

    // Compute activation date with fallback
    def activationMillis = null
    if (record.activation_date instanceof String)
        activationMillis = parseDateMillis(record.activation_date)
    if (!activationMillis && record.first_seen_date instanceof Number)
        activationMillis = record.first_seen_date as Long
    if (!activationMillis) errors << "activation_date and first_seen_date missing or invalid"

    // Compute hash bucket and partition
    Integer bucket = null, year = null, month = null
    if (id && activationMillis) {
        byte[] idBytes = id.getBytes(StandardCharsets.UTF_8)
        int hash = MurmurHash3.hash32(idBytes, 0, idBytes.length, 0)
        bucket = Math.abs(hash) % 12
        def dt = Instant.ofEpochMilli(activationMillis).atZone(tunisZone)
        year = dt.getYear()
        month = dt.getMonthValue()
    }

    // Build output record (always)
    def outputRec = [
        id                  : id ?: null,
        ICCID               : iccid ?: null,
        SN                  : sn ?: null,
        MSISDN              : msisdn ?: null,
        IMSI                : imsi ?: null,
        mvno_id             : mvno ?: null,
        activation_date     : activationMillis ?: 0L,
        customer_id         : "unknown",
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : record.source_system ?: null,
        partition           : (bucket != null && year != null && month != null) ? [bucket, year, month] : null,
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join("; ")
    ]

    outputRecords << outputRec
}

// Write to output FlowFile
FlowFile outputFlowFile = session.create(inputFlowFile)
outputFlowFile = session.write(outputFlowFile, { _, os ->
    os.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Set attributes
outputFlowFile = session.putAttribute(outputFlowFile, 'target_iceberg_table_name', 'sims')
outputFlowFile = session.putAttribute(outputFlowFile, 'schema.name', 'sims')
outputFlowFile = session.putAttribute(outputFlowFile, 'record.count', outputRecords.size().toString())

session.transfer(outputFlowFile, REL_SUCCESS)
session.remove(inputFlowFile)
log.info("Transferred ${outputRecords.size()} sims records with validation flags")
