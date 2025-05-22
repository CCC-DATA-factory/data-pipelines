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

// Prepare combined output list
List outputRecords = []

records.each { record ->
    def id = record['id']
    def name = record['name']
    def isValid = false
    def comment = null

    // Initialize all fields with existing or null values
    def transformed = [
        id                  : (id != null ? id.toString() : null),
        name                : (name ?: null),
        first_seen_date     : record.first_seen_date ?: null,
        ingestion_date      : record.ingestion_date ?: null,
        transformation_date : nowTunisMillis,
        source_system       : record.source_system ?: null,
        partition           : null
    ]

    // Validation rules
    if ((id instanceof Integer || (id instanceof String && id.isInteger())) &&
        name instanceof String && name?.trim()) {
        isValid = true
    } else {
        isValid = false
        comment = "Invalid id or name"
    }

    // Add validation attributes
    transformed['is_valid'] = isValid
    transformed['comment'] = comment

    outputRecords << transformed
}

// Write combined JSON to new FlowFile and transfer
def resultFlowFile = session.create(flowFile)
resultFlowFile = session.write(resultFlowFile, { _ ,outStream ->
    outStream.write(JsonOutput.toJson(outputRecords).getBytes('UTF-8'))
} as StreamCallback)

// Inherit attributes
def newAttrs = [:]
inheritedAttributes.each { attr ->
    flowFile.getAttribute(attr)?.with { newAttrs[attr] = it }
}

// Add metadata attributes
newAttrs['file.size'] = String.valueOf(JsonOutput.toJson(outputRecords).getBytes('UTF-8').length)
newAttrs['records.count'] = String.valueOf(outputRecords.size())
newAttrs['target_iceberg_table_name'] = 'mvnos'
newAttrs['schema.name'] = 'mvnos'

newAttrs.each { k, v -> 
    resultFlowFile = session.putAttribute(resultFlowFile, k, v)
}

session.transfer(resultFlowFile, REL_SUCCESS)
