import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import java.time.ZoneId
import java.time.ZonedDateTime

def flowFile = session.get()
if (!flowFile) return

// Get current Tunis timestamp
long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Read and parse input JSON
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
    def transformed = [:]

    // Validation rules
    if ((id instanceof Integer || (id instanceof String && id.isInteger())) &&
        name instanceof String && name?.trim()) {
        // valid
        isValid = true
        // Convert id to string
        def idStr = id.toString()
        // Transformation logic
        transformed = [
            id                  : idStr,
            name                : name,
            first_seen_date     : record.first_seen_date ?: null,
            ingestion_date      : record.ingestion_date ?: null,
            transformation_date : nowTunisMillis,
            source_system       : record.source_system ?: null,
            partition           : null
        ]
    } else {
        // invalid
        isValid = false
        comment = "Invalid id or name"
    }

    // Build output record: merge original or transformed fields
    def outRec = new LinkedHashMap<>(transformed)

    // Add validation attributes
    outRec['is_valid'] = isValid
    outRec['comment'] = comment

    outputRecords << outRec
}

// Write combined JSON to new FlowFile and transfer
def resultFlowFile = session.create(flowFile)
resultFlowFile = session.write(resultFlowFile, { _ ,outStream ->
    outStream.write(JsonOutput.toJson(outputRecords).getBytes('UTF-8'))
} as StreamCallback)
session.transfer(resultFlowFile, REL_SUCCESS)
// Remove original
session.remove(flowFile)
