import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.text.SimpleDateFormat
import java.util.TimeZone
import java.time.*
import org.apache.nifi.processor.io.StreamCallback
import org.apache.nifi.flowfile.FlowFile
import org.apache.nifi.processor.Relationship


def inheritedAttributes = ['filepath', 'database_name', 'collection_name']



// read entire FlowFile to String
String rawJson = ''
flowFile = session.write(flowFile, { InputStream inStream, OutputStream outStream ->
    rawJson = IOUtils.toString(inStream, StandardCharsets.UTF_8)
    outStream.write(rawJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// parse input JSON
def parser = new JsonSlurper()
List inputRecords
try {
    inputRecords = parser.parseText(rawJson)
    if (!(inputRecords instanceof List)) {
        throw new Exception("Expected a JSON array of records")
    }
} catch (Exception e) {
    log.error("Invalid incoming JSON", e)
    flowFile = session.putAttribute(flowFile, 'error', "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

// prepare accumulators
List allBundles = []
List allPriceHistories = []
long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// helper to process a single record
inputRecords.eachWithIndex { data, idx ->
    def errors = []
    // override first price_history date
    try {
        def sdf = new SimpleDateFormat('yyyy-MM-dd')
        sdf.timeZone = TimeZone.getTimeZone("UTC")
        long overrideMillis = sdf.parse('2023-10-06').time
        if (data.price_history && data.price_history[0]?.date instanceof String) {
            data.price_history[0].date = overrideMillis.toString()
        }
    } catch(Exception e) {
        log.warn("Could not override price_history date: ${e.message}")
    }
    // validations
    if (!data.name?.trim())                        errors << 'name is required'
    if (!data.bundleId)                            errors << 'bundleId is required'
    if (!(data.price?.amount instanceof Number))   errors << 'price.amount missing or invalid'
    if (!(data.validity?.number instanceof Number))errors << 'validity.number missing or invalid'
    if (!data.createdAt && !data.first_seen_date)  errors << 'createdAt/first_seen_date missing'
    if (!( (data.content?.data?.amount instanceof Number) 
        || (data.content?.voice?.amount instanceof Number) 
        || (data.content?.sms instanceof Number) )) {
        errors << 'At least one of content.data.amount, content.voice.amount, content.sms is required'
    }
    // base transform
    def bundleObj = [
        id                  : data._id,
        name                : data.name,
        bundleid            : data.bundleId?.toString(),
        data_amount_gb      : null,
        voice_amount_minutes: null,
        sms_amount          : data.content?.sms ?: 0,
        validity_days       : data.validity?.number,
        createdat           : null,
        first_seen_date     : data.first_seen_date,
        ingestion_date      : data.ingestion_date,
        transformation_date : nowMillis,
        source_system       : data.source_system,
        partition           : null
    ]
    // compute units and created at only if valid so far
    if (!errors) {
        try {
            def dataAmt   = data.content?.data?.amount ?: 0
            def dataUnit  = data.content?.data?.unit ?: 'Megabytes'
            bundleObj.data_amount_gb = (dataUnit == 'Megabytes') ? dataAmt/1000.0 : dataAmt
            def voiceAmt  = data.content?.voice?.amount ?: 0
            def voiceUnit = data.content?.voice?.unit ?: 'Minutes'
            bundleObj.voice_amount_minutes = (voiceUnit == 'Hours') ? voiceAmt*60 : voiceAmt
            def createdRaw = data.createdAt ?: data.first_seen_date
            long createdMillis = createdRaw.toString().toLong()
            bundleObj.createdat = createdMillis
        } catch(Exception e) {
            errors << "Transformation error: ${e.message}"
        }
    }
    // add validation fields
    bundleObj.is_valid = errors.isEmpty()
    bundleObj.comment = errors.isEmpty() ? null : errors.join('; ')
    // add to bundles
    allBundles << bundleObj
    // build price history entries even if invalid bundle
    (data.price_history ?: []).each { rec ->
        def ph = [
            bundleid           : bundleObj.bundleid,
            price              : rec.price,
            start_date         : null,
            end_date           : null,
            first_seen_date    : data.first_seen_date,
            ingestion_date     : data.ingestion_date,
            transformation_date: nowMillis,
            source_system      : data.source_system,
            is_valid           : bundleObj.is_valid,
            comment            : bundleObj.comment,
            partition          : null
        ]
        try {
            long dt = rec.date.toString().toLong()
            ph.end_date = dt
            ph.start_date = dt
        } catch(_) {
            // leave dates null
        }
        allPriceHistories << ph
    }
}

// helper to branch data into new FlowFile
def branch = { List data, Relationship rel, String tableName ->
    if (!data) return
    FlowFile out = session.create(flowFile)
    out = session.write(out, { _, os ->
        os.write(JsonOutput.toJson(data).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    def attrs = [:]
    inheritedAttributes.each { k -> flowFile.getAttribute(k)?.with { attrs[k] = it } }
    attrs['file.size'] = String.valueOf(JsonOutput.toJson(data).bytes.length)
    attrs['records.count'] = String.valueOf(data.size())
    attrs['target_iceberg_table_name'] = tableName
    attrs['schema.name'] = tableName
    attrs.each { k,v -> out = session.putAttribute(out, k, v) }
    session.transfer(out, rel)
}

// write branches
branch(allBundles,        REL_SUCCESS, 'bundles')
branch(allPriceHistories, REL_SUCCESS, 'bundles_price_history')

