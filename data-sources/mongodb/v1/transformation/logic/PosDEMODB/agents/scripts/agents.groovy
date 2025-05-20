import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.processor.io.OutputStreamCallback
import org.apache.nifi.flowfile.FlowFile


def inheritedAttrs = ['filepath', 'database_name', 'collection_name']



def rawJson = ''
flowFile = session.write(flowFile, { inputStream, outputStream ->
    rawJson = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
    outputStream.write(rawJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def parser = new JsonSlurper()
List records
try {
    records = parser.parseText(rawJson)
    if (!(records instanceof List)) {
        throw new Exception("Expected JSON array of records")
    }
} catch(Exception e) {
    log.error("Failed to parse input JSON", e)
    flowFile = session.putAttribute(flowFile, 'error', "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()
List outputRecords = []

records.eachWithIndex { rec, idx ->
    def errs = []
    if (!rec._id)         errs << '_id is required'
    if (!rec.first_name)  errs << 'first_name is required'
    if (!rec.last_name)   errs << 'last_name is required'

    long joinedAt = 0L
    if (rec.created_at != null) {
        try {
            joinedAt = rec.created_at instanceof Number ? rec.created_at.longValue() : rec.created_at.toString().toLong()
        } catch(Exception e) {
            errs << "Invalid created_at value: ${rec.created_at}"
        }
    } else if (rec.first_seen_date != null) {
        try {
            joinedAt = rec.first_seen_date instanceof Number ? rec.first_seen_date.longValue() : rec.first_seen_date.toString().toLong()
        } catch(Exception e) {
            errs << "Invalid first_seen_date value: ${rec.first_seen_date}"
        }
    } else {
        errs << 'Either created_at or first_seen_date must be present'
    }

    def isValid = errs.isEmpty()
    def outRec = [
        id                  : rec._id?.toString(),
        name                : (rec.first_name?.toString()?.trim() ?: "") + " " + (rec.last_name?.toString()?.trim() ?: ""),
        role                : 'agent',
        parent_id           : rec.reseller?.toString(),
        shop_name           : null,
        joined_at           : joinedAt,
        first_seen_date     : rec.first_seen_date,
        ingestion_date      : rec.ingestion_date,
        transformation_date : nowMillis,
        source_system       : rec.source_system,
        is_valid            : isValid,
        comment             : isValid ? null : errs.join('; '),
        partition           : null
    ]
    outputRecords << outRec

    if (!isValid) log.warn("Record $idx invalid: ${errs.join('; ')}")
}

FlowFile outFF = session.create(flowFile)
outFF = session.write(outFF, { out ->
    out.write(JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8))
} as OutputStreamCallback)

def attrs = [:]
inheritedAttrs.each { k ->
    def v = flowFile.getAttribute(k)
    if (v) attrs[k] = v
}
def jsonBytes = JsonOutput.toJson(outputRecords).getBytes(StandardCharsets.UTF_8)
attrs['file.size'] = String.valueOf(jsonBytes.length)
attrs['records.count'] = String.valueOf(outputRecords.size())
attrs['target_iceberg_table_name'] = "sallers"
attrs["schema.name"] =  "sallers"
attrs.each { k,v -> outFF = session.putAttribute(outFF, k, v) }

session.transfer(outFF, REL_SUCCESS)
