import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import java.time.*

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
        log.error("Expected a list of objects but got: " + records.getClass().getName())
        session.transfer(inputFlowFile, REL_FAILURE)
        return
    }
} catch (Exception e) {
    log.error("Failed to parse input JSON", e)
    session.transfer(inputFlowFile, REL_FAILURE)
    return
}

ZoneId tunisZone = ZoneId.of("Africa/Tunis")
long nowMillis = ZonedDateTime.now(tunisZone).toInstant().toEpochMilli()

List transformedRecords = []
List rejectedRecords = []

records.each { record ->
    if (!record.completed) {
        record['_error'] = "Not completed"
        rejectedRecords << record
        return
    }

    Long createdAtMillis = null
    if (record.creation_date != null && record.creation_date.isLong()) {
        // Convert UTC millis to Tunisian millis
        def utcMillis = record.creation_date as Long
        createdAtMillis = ZonedDateTime.ofInstant(Instant.ofEpochMilli(utcMillis), ZoneOffset.UTC)
                            .withZoneSameInstant(tunisZone)
                            .toInstant().toEpochMilli()
    } else if (record.first_seen_date != null) {
        createdAtMillis = record.first_seen_date as Long // Already in Tunisian time
    }

    if (!record._id || !record.SIM || !record.agent || !record.customer || createdAtMillis == null) {
        record['_error'] = "Missing required fields or createdAt is null"
        rejectedRecords << record
        return
    }

    def transformed = [
        id                  : record._id,
        sim_id              : record.SIM,
        agent_id            : record.agent,
        customer_id         : record.customer,
        mvno_id             : record.mvno_id?.toString(),
        createdAt           : createdAtMillis,
        shopName            : record.shopName ?: "",
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : record.source_system ?: null
    ]
    transformedRecords << transformed
}

if (!transformedRecords.isEmpty()) {
    def successFlowFile = session.create(inputFlowFile)
    successFlowFile = session.write(successFlowFile, { out ->
        out.write(JsonOutput.toJson(transformedRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)

    successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "activation")
    successFlowFile = session.putAttribute(successFlowFile, "schema.name", "activation")
    successFlowFile = session.putAttribute(successFlowFile, "record.count", transformedRecords.size().toString())
    session.transfer(successFlowFile, REL_SUCCESS)
    log.info("Transferred ${transformedRecords.size()} valid activation records")
}

if (!rejectedRecords.isEmpty()) {
    def failureFlowFile = session.create(inputFlowFile)
    failureFlowFile = session.write(failureFlowFile, { out ->
        out.write(JsonOutput.toJson(rejectedRecords).getBytes(StandardCharsets.UTF_8))
    } as OutputStreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, "error", "Rejected ${rejectedRecords.size()} activation records")
    session.transfer(failureFlowFile, REL_FAILURE)
    log.warn("Rejected ${rejectedRecords.size()} activation records")
}

session.remove(inputFlowFile)
