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

// setup
 def log = log
 def session = session
 def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// fetch incoming
FlowFile inFF = session.get()
if (!inFF) return

// read entire FlowFile to String
String rawJson = ''
inFF = session.write(inFF, { InputStream inStream, OutputStream outStream ->
    rawJson = IOUtils.toString(inStream, StandardCharsets.UTF_8)
    // write back the same content so inFF remains unchanged
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
    inFF = session.putAttribute(inFF, 'error', "Invalid JSON: ${e.message}")
    session.transfer(inFF, REL_FAILURE)
    return
}

// prepare accumulators
def allBundles = []
def allPriceHistories = []
def failed      = []
long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// helper to process a single record
def processBundle = { Map data ->
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
    def errors = []
    if (!data.name?.trim())                                errors << 'name is required'
    if (!data.bundleId)                                    errors << 'bundleId is required'
    if (!(data.price?.amount instanceof Number))           errors << 'price.amount missing or invalid'
    if (!(data.validity?.number instanceof Number))        errors << 'validity.number missing or invalid'
    if (!data.createdAt && !data.first_seen_date)          errors << 'createdAt/first_seen_date missing'
    
    // require at least one content field
    if (!( (data.content?.data?.amount instanceof Number) 
        || (data.content?.voice?.amount instanceof Number) 
        || (data.content?.sms instanceof Number) )) {
        errors << 'At least one of content.data.amount, content.voice.amount, content.sms is required'
    }

    if (errors) throw new Exception("Validation errors: ${errors.join('; ')}")

    // compute units
    def dataAmt   = data.content?.data?.amount ?: 0
    def dataUnit  = data.content?.data?.unit ?: 'Megabytes'
    def dataGB    = (dataUnit == 'Megabytes') ? dataAmt/1000.0 : dataAmt
    def voiceAmt  = data.content?.voice?.amount ?: 0
    def voiceUnit = data.content?.voice?.unit ?: 'Minutes'
    def voiceMin  = (voiceUnit == 'Hours') ? voiceAmt*60 : voiceAmt

    def createdRaw    = data.createdAt ?: data.first_seen_date
    long createdMillis = createdRaw.toString().toLong()

    // build bundle object
    def bundleObj = [
        id                  : data._id,
        name                : data.name,
        bundleid            : data.bundleId.toString(),
        data_amount_gb      : dataGB,
        voice_amount_minutes: voiceMin,
        sms_amount          : data.content?.sms ?: 0,
        validity_days       : data.validity.number,
        createdat           : createdMillis,
        first_seen_date     : data.first_seen_date,
        ingestion_date      : data.ingestion_date,
        transformation_date : nowMillis,
        source_system       : data.source_system
    ]

    // build and sort price history list
    def hist = []
    (data.price_history ?: []).each { rec ->
        try {
            long dt = rec.date.toString().toLong()
            hist << [ bundleid: bundleObj.bundleid, price: rec.price, date: dt ]
        } catch(_) { /* skip invalid entries */ }
    }
    hist.sort { it.date }

    // build price intervals
    def priceArray = []
    hist.eachWithIndex { e, i ->
        long start = (i==0 ? createdMillis : hist[i-1].date)
        long end   = e.date
        if (start > end) start = end
        priceArray << [
            bundleid           : bundleObj.bundleid,
            price              : e.price,
            start_date         : start,
            end_date           : end,
            first_seen_date    : data.first_seen_date,
            ingestion_date     : data.ingestion_date,
            transformation_date: nowMillis,
            source_system      : data.source_system
        ]
    }
    // add final open-ended interval
    priceArray << [
        bundleid           : bundleObj.bundleid,
        price              : data.price.amount,
        start_date         : (hist ? hist[-1].date : createdMillis),
        end_date           : 253402300799000L,
        first_seen_date    : data.first_seen_date,
        ingestion_date     : data.ingestion_date,
        transformation_date: nowMillis,
        source_system      : data.source_system
    ]

    return [ bundleObj, priceArray ]
}

// process each record
inputRecords.eachWithIndex { rec, idx ->
    try {
        def (b, pArr) = processBundle(rec)
        allBundles << b
        allPriceHistories.addAll(pArr)
    } catch(Exception e) {
        failed << [ index: idx, record: rec, error: e.message ]
        log.error("Record $idx failed: ${e.message}", e)
    }
}

// helper to branch data into new FlowFile
def branch = { List data, Relationship rel, String tableName ->
    if (!data) return
    FlowFile out = session.create(inFF)
    out = session.write(out, { _, os ->
        os.write(JsonOutput.toJson(data).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    def attrs = [:]
    inheritedAttributes.each { k ->
        def v = inFF.getAttribute(k)
        if (v) attrs[k] = v
    }
    attrs['file.size']                 = String.valueOf(JsonOutput.toJson(data).bytes.length)
    attrs['records.count']             = String.valueOf(data.size())
    attrs['target_iceberg_table_name'] = tableName
    attrs['schema.name'] = tableName
    attrs.each { k,v -> out = session.putAttribute(out, k, v) }
    session.transfer(out, rel)
}

// write branches
branch(allBundles,        REL_SUCCESS, 'bundles')
branch(allPriceHistories, REL_SUCCESS, 'bundles_price_history')
branch(failed,            REL_FAILURE, 'failed_records')

// remove original
session.remove(inFF)