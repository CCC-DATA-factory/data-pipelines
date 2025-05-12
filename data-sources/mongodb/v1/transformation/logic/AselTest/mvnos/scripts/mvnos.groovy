import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import org.apache.nifi.processor.io.StreamCallback
import java.time.ZoneId
import java.time.ZonedDateTime

def flowFile = session.get()
if (!flowFile) return

def validRecords = []
def invalidRecords = []

// Get current Tunis timestamp
long nowTunisMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

flowFile = session.write(flowFile, { inputStream, outputStream ->
    def parser = new JsonSlurper()
    def records = parser.parse(inputStream)

    records.each { record ->
        def id = record['id']
        def name = record['name']

        // Validation rules
        if ((id instanceof Integer || (id instanceof String && id.isInteger())) &&
            name instanceof String && name?.trim()) {

            // Convert id to string if needed
            def idStr = id.toString()

            // Transformation
            def transformed = [
                id                  : idStr,
                name                : name,
                first_seen_date     : record.first_seen_date ?: null,
                ingestion_date      : record.ingestion_date ?: null,
                transformation_date : nowTunisMillis,
                source_system       : record.source_system ?: null
            ]
            validRecords << transformed
        } else {
            invalidRecords << record
        }
    }

    // Dummy write, will replace content via cloning
    outputStream.write("[]".getBytes("UTF-8"))
} as StreamCallback)

// Create Success flowfile with valid records
if (validRecords) {
    def successFlowFile = session.clone(flowFile)
    successFlowFile = session.write(successFlowFile, { out ->
        out.write(JsonOutput.toJson(validRecords).getBytes("UTF-8"))
    } as OutputStreamCallback)
    session.transfer(successFlowFile, REL_SUCCESS)
}

// Create Failure flowfile with invalid records
if (invalidRecords) {
    def failureFlowFile = session.clone(flowFile)
    failureFlowFile = session.write(failureFlowFile, { out ->
        out.write(JsonOutput.toJson(invalidRecords).getBytes("UTF-8"))
    } as OutputStreamCallback)
    session.transfer(failureFlowFile, REL_FAILURE)
}

// Remove original
session.remove(flowFile)
