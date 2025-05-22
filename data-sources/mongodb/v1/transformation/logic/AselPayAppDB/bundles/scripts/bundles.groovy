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

String rawJson = ''
flowFile = session.write(flowFile, { InputStream inStream, OutputStream outStream ->
    rawJson = IOUtils.toString(inStream, StandardCharsets.UTF_8)
    outStream.write(rawJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

def parser = new JsonSlurper()
List inputRecords
try {
    inputRecords = parser.parseText(rawJson)
    if (!(inputRecords instanceof List)) throw new Exception("Expected a JSON array of records")
} catch (Exception e) {
    log.error("Invalid incoming JSON", e)
    flowFile = session.putAttribute(flowFile, 'error', "Invalid JSON: ${e.message}")
    session.transfer(flowFile, REL_FAILURE)
    return
}

List allBundles = []
List allPriceHistories = []
long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

def parseLongSafe = { obj ->
    try {
        return obj?.toString()?.toLong()
    } catch (_) {
        return null
    }
}

def overrideFirstDate = '2023-10-06'
def sdf = new SimpleDateFormat('yyyy-MM-dd')
sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
long overrideMillis = sdf.parse(overrideFirstDate).time

inputRecords.eachWithIndex { data, idx ->
    def errors = []

    if (!data.name?.trim()) errors << 'name is required'
    if (!data.bundleId) errors << 'bundleId is required'
    if (!(data.price?.amount instanceof Number)) errors << 'price.amount missing or invalid'
    if (!(data.validity?.number instanceof Number)) errors << 'validity.number missing or invalid'
    if (!data.createdAt && !data.first_seen_date) errors << 'createdAt/first_seen_date missing'
    if (!((data.content?.data?.amount instanceof Number) ||
          (data.content?.voice?.amount instanceof Number) ||
          (data.content?.sms instanceof Number))) {
        errors << 'At least one of content.data.amount, content.voice.amount, content.sms is required'
    }

    def createdRaw = data.createdAt ?: data.first_seen_date
    long createdMillis = parseLongSafe(createdRaw) ?: nowMillis

    def bundleObj = [
        id                  : data._id ?: null,
        name                : data.name ?: null,
        bundleid            : data.bundleId?.toString() ?: null,
        data_amount_gb      : null,
        voice_amount_minutes: null,
        sms_amount          : data.content?.sms ?: null,
        validity_days       : data.validity?.number ?: null,
        createdat           : createdMillis,
        first_seen_date     : data.first_seen_date ?: null,
        ingestion_date      : data.ingestion_date ?: null,
        transformation_date : nowMillis,
        source_system       : data.source_system ?: null,
        partition           : null,
        is_valid            : errors.isEmpty(),
        comment             : errors.isEmpty() ? null : errors.join('; ')
    ]

    if (errors.isEmpty()) {
        try {
            def dataAmt  = data.content?.data?.amount ?: 0
            def dataUnit = data.content?.data?.unit ?: 'Megabytes'
            bundleObj.data_amount_gb = (dataUnit == 'Megabytes') ? dataAmt/1000.0 : dataAmt

            def voiceAmt  = data.content?.voice?.amount ?: 0
            def voiceUnit = data.content?.voice?.unit ?: 'Minutes'
            bundleObj.voice_amount_minutes = (voiceUnit == 'Hours') ? voiceAmt*60 : voiceAmt
        } catch(Exception e) {
            errors << "Transformation error: ${e.message}"
        }
    }

    allBundles << bundleObj

    def history = []
    (data.price_history ?: []).eachWithIndex { rec, i ->
        def dateVal = (i == 0) ? overrideMillis : parseLongSafe(rec.date)
        if (dateVal != null) {
            history << [ date: dateVal, price: rec.price ]
        }
    }
    history.sort { it.date }

    history.eachWithIndex { h, i ->
        long start = (i == 0) ? createdMillis : history[i - 1].date
        long end   = h.date
        if (start > end) start = end

        def ph = [
            bundleid           : bundleObj.bundleid,
            price              : h.price ?: null,
            start_date         : start,
            end_date           : end,
            first_seen_date    : data.first_seen_date ?: null,
            ingestion_date     : data.ingestion_date ?: null,
            transformation_date: nowMillis,
            source_system      : data.source_system ?: null,
            is_valid           : bundleObj.is_valid,
            comment            : bundleObj.comment,
            partition          : null
        ]
        allPriceHistories << ph
    }

    if (history) {
        def last = history[-1].date
        def ph = [
            bundleid           : bundleObj.bundleid,
            price              : data.price?.amount ?: null,
            start_date         : last,
            end_date           : 253402300799000,
            first_seen_date    : data.first_seen_date ?: null,
            ingestion_date     : data.ingestion_date ?: null,
            transformation_date: nowMillis,
            source_system      : data.source_system ?: null,
            is_valid           : bundleObj.is_valid,
            comment            : bundleObj.comment,
            partition          : null
        ]
        allPriceHistories << ph
    }
}

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

branch(allBundles,        REL_SUCCESS, 'bundles')
branch(allPriceHistories, REL_SUCCESS, 'bundles_price_history')
