import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import java.time.ZoneId
import java.time.ZonedDateTime

// Attributes to inherit
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

String inputJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    inputJson = inputStream.getText('UTF-8')
    // dummy write; content replaced later
    outputStream.write("[]".getBytes('UTF-8'))
} as StreamCallback)

def parser = new JsonSlurper()
def records = parser.parseText(inputJson)

// Load attribute metadata
def databaseName = flowFile.getAttribute('database_name') ?: 'unknown_database'
def collectionName = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

// Prepare output lists
List outputRecords = []
List invalidRecordsSummary = []

records.each { record ->
    def id = record['id']
    def name = record['name']
    def isValid = false
    def comment = null

    def transformed = [
        id                  : (id != null ? id.toString() : null),
        name                : (name ?: null),
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null
    ]

    if ((id instanceof Integer || (id instanceof String && id.isInteger())) &&
        name instanceof String && name?.trim()) {
        isValid = true
    } else {
        isValid = false
        comment = "Invalid id or name"
        invalidRecordsSummary << [
            database_name   : databaseName,
            collection_name : collectionName,
            record_id       : id?.toString() ?: null,
            error_message   : comment
        ]
    }

    transformed['is_valid'] = isValid
    transformed['comment'] = comment
    outputRecords << transformed
}

// Write transformed records to new FlowFile
def resultFlowFile = session.create(flowFile)
resultFlowFile = session.write(resultFlowFile, { _, outStream ->
    outStream.write(JsonOutput.toJson(outputRecords).getBytes('UTF-8'))
} as StreamCallback)

// Inherit attributes
def newAttrs = [:]
inheritedAttributes.each { attr -> flowFile.getAttribute(attr)?.with { newAttrs[attr] = it } }

// Add output metadata

newAttrs['target_iceberg_table_name'] = 'mvnos'
newAttrs['schema.name'] = 'mvnos'

// Set attributes and transfer
newAttrs.each { k, v -> resultFlowFile = session.putAttribute(resultFlowFile, k, v) }
session.transfer(resultFlowFile, REL_SUCCESS)

// Write invalid summary if any
if (!invalidRecordsSummary.isEmpty()) {
    def failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { _, out ->
        out.write(JsonOutput.toJson(invalidRecordsSummary).getBytes('UTF-8'))
    } as StreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', invalidRecordsSummary.size().toString() )
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type', 'validation_summary')
    inheritedAttributes.each { k -> flowFile.getAttribute(k)?.with { failureFlowFile = session.putAttribute(failureFlowFile, k, it) } }
    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${invalidRecordsSummary.size()} validation errors to REL_FAILURE")
}
