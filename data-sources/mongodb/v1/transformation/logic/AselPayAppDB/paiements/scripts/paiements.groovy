import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import java.time.*
import org.apache.nifi.flowfile.FlowFile


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

    // Always initialize all fields, even if validation fails
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
        source_system       : record.source_system ?: null,
        partition           : createdMillis != null ? [
                                Instant.ofEpochMilli(createdMillis).atZone(tunisZone).getYear(),
                                Instant.ofEpochMilli(createdMillis).atZone(tunisZone).getMonthValue(),
                                Instant.ofEpochMilli(createdMillis).atZone(tunisZone).getDayOfMonth()
                              ] : null
    ]

    // Validation rules
    if (!record._id) errorMessages << "_id missing"
    if (!record.commercial) errorMessages << "commercial missing"
    if (!record.franchise) errorMessages << "franchise missing"
    if (!record.transaction) errorMessages << "transaction missing"
    if (record.amount == null) errorMessages << "amount missing"

    // Add validation flags
    outputRec['is_valid'] = errorMessages.isEmpty()
    outputRec['comment'] = errorMessages.isEmpty() ? null : errorMessages.join("; ")

    outputRecords << outputRec
}

// Write all records (valid and invalid) to success
FlowFile successFlowFile = session.create(flowFile)
successFlowFile = session.write(successFlowFile, { _ , out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

successFlowFile = session.putAttribute(successFlowFile, "target_iceberg_table_name", "paiement")
successFlowFile = session.putAttribute(successFlowFile, "schema.name", "paiement")
successFlowFile = session.putAttribute(successFlowFile, "record.count", outputRecords.size().toString())

session.transfer(successFlowFile, REL_SUCCESS)
log.info("Transferred ${outputRecords.size()} records with validation flags")
