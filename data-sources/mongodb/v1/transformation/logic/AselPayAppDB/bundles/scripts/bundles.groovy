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

// Inherited attributes to carry over
def inheritedAttributes = ['filepath', 'database_name', 'collection_name']

// Read raw JSON from incoming FlowFile
String rawJson = ''
flowFile = session.write(flowFile, { InputStream inStream, OutputStream outStream ->
    rawJson = IOUtils.toString(inStream, StandardCharsets.UTF_8)
    outStream.write(rawJson.getBytes(StandardCharsets.UTF_8))
} as StreamCallback)

// Parse JSON array
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

// Load metadata attributes
def databaseName   = flowFile.getAttribute('database_name')   ?: 'unknown_database'
def collectionName = flowFile.getAttribute('collection_name') ?: 'unknown_collection'

// Prepare lists for output
List allBundles         = []
List allPriceHistories  = []
List invalidRecordsSummary = []

// Current timestamp in millis (Africa/Tunis timezone)
long nowMillis = ZonedDateTime.now(ZoneId.of("Africa/Tunis")).toInstant().toEpochMilli()

// Safe long parser
def parseLongSafe = { obj ->
    try {
        return obj?.toString()?.toLong()
    } catch (_) {
        return null
    }
}
long utcToTunisLocalEpochMillis(long utcEpochMillis) {
    ZoneId tunisZone = ZoneId.of("Africa/Tunis")

    // Convert UTC millis to Instant
    Instant instant = Instant.ofEpochMilli(utcEpochMillis)

    // Get the ZonedDateTime in Tunisia timezone for that instant
    ZonedDateTime tunisZoned = instant.atZone(tunisZone)

    // Get local date/time components
    LocalDateTime tunisLocalDateTime = tunisZoned.toLocalDateTime()

    // Now interpret that local date/time as if it were UTC, get the epoch millis
    long shiftedMillis = tunisLocalDateTime.atZone(ZoneOffset.UTC).toInstant().toEpochMilli()

    return shiftedMillis
}

// Override date for price history splits
def overrideFirstDate = '2023-10-06'
def sdf = new SimpleDateFormat('yyyy-MM-dd')
sdf.setTimeZone(TimeZone.getTimeZone("UTC"))
long overrideMillis = sdf.parse(overrideFirstDate).time

// Process each bundle record
inputRecords.eachWithIndex { data, idx ->
    def errors = []

    // Basic validations
    if (!data.name?.trim())                     errors << 'name is required'
    if (!data.bundleId)                         errors << 'bundleId is required'
    if (!(data.price?.amount instanceof Number)) errors << 'price.amount missing or invalid'
    if (!(data.validity?.number instanceof Number)) errors << 'validity.number missing or invalid'
    if (!data.createdAt && !data.first_seen_date) errors << 'createdAt/first_seen_date missing'
    if (!((data.content?.data?.amount instanceof Number) ||
          (data.content?.voice?.amount instanceof Number) ||
          (data.content?.sms instanceof Number))) {
        errors << 'At least one of content.data.amount, content.voice.amount, content.sms is required'
    }

    // Determine created_at timestamp
    long createdMillis

    if (data.createdAt) {
        // Parse createdAt to long, then convert to Tunisian local millis
        def createdAtLong = parseLongSafe(data.createdAt)
        if (createdAtLong != null) {
            createdMillis = utcToTunisLocalEpochMillis(createdAtLong)
        } else {
            createdMillis = nowMillis
        }
    } else {
        // Use first_seen_date parsed as long or fallback
        createdMillis = parseLongSafe(data.first_seen_date) ?: nowMillis
    }
    // Build bundle object
    def bundleObj = [
        id                   : data._id ?: null,
        name                 : data.name ?: null,
        bundleid             : data.bundleId?.toString() ?: null,
        data_amount_gb       : null,
        voice_amount_minutes : null,
        sms_amount           : data.content?.sms ?: null,
        validity_days        : data.validity?.number ?: null,
        created_at           : createdMillis,
        first_seen_date      : data.first_seen_date ?: null,
        ingestion_date       : data.ingestion_date ?: null,
        transformation_date  : nowMillis,
        source_system        : data.source_system ?: null,
        is_valid             : errors.isEmpty(),
        comment              : errors.isEmpty() ? null : errors.join('; ')
    ]

    // Collect invalid records for failure channel
    if (!errors.isEmpty()) {
        invalidRecordsSummary << [
            database_name   : databaseName,
            collection_name : collectionName,
            record_id       : data._id ?: null,
            error_message   : errors.join('; ')
        ]
    }

    // Compute additional fields if valid
    if (errors.isEmpty()) {
        try {
            def dataAmt  = data.content?.data?.amount ?: 0
            def dataUnit = data.content?.data?.unit   ?: 'Megabytes'
            bundleObj.data_amount_gb = (dataUnit == 'Megabytes') ? dataAmt/1000.0 : dataAmt

            def voiceAmt  = data.content?.voice?.amount ?: 0
            def voiceUnit = data.content?.voice?.unit   ?: 'Minutes'
            bundleObj.voice_amount_minutes = (voiceUnit == 'Hours') ? voiceAmt*60 : voiceAmt
        } catch(Exception e) {
            bundleObj.comment += "; transformation error: ${e.message}"
        }
    }

    // Add to bundles list
    allBundles << bundleObj

    // Build price history entries per your logic
    def priceHistoryList = []

    if (data.price_history && data.price_history instanceof List && data.price_history.size() > 0) {
        // 1) Historic price entry: created_at -> override date
        def firstHistoricPrice = data.price_history[0]?.price ?: data.price?.amount
        priceHistoryList << [
            price     : firstHistoricPrice,
            start_date: createdMillis,
            end_date  : overrideMillis
        ]

        // 2) Current price entry: override date -> far future
        priceHistoryList << [
            price     : data.price?.amount ?: null,
            start_date: overrideMillis,
            end_date  : 253402300799000
        ]
    } else {
        // No price_history present: single entry with current price
        priceHistoryList << [
            price     : data.price?.amount ?: null,
            start_date: createdMillis,
            end_date  : 253402300799000
        ]
    }

    // Append to allPriceHistories
    priceHistoryList.each { h ->
        def ph = [
            bundleid           : bundleObj.id,
            price              : h.price,
            start_date         : h.start_date,
            end_date           : h.end_date,
            first_seen_date    : data.first_seen_date ?: null,
            ingestion_date     : data.ingestion_date     ?: null,
            transformation_date: nowMillis,
            source_system      : data.source_system      ?: null,
            is_valid           : bundleObj.is_valid,
            comment            : bundleObj.comment
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
    attrs['target_iceberg_table_name'] = tableName
    attrs['schema.name']              = tableName
    attrs.each { k, v -> out = session.putAttribute(out, k, v) }
    session.transfer(out, rel)
}


// Emit bundles and price history
branch(allBundles,        REL_SUCCESS, 'bundles')
branch(allPriceHistories, REL_SUCCESS, 'bundle_price_history')

// Emit validation errors if any
if (!invalidRecordsSummary.isEmpty()) {
    FlowFile failureFlowFile = session.create(flowFile)
    failureFlowFile = session.write(failureFlowFile, { _, out ->
        out.write(JsonOutput.toJson(invalidRecordsSummary).getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.count', invalidRecordsSummary.size().toString())
    failureFlowFile = session.putAttribute(failureFlowFile, 'error.type',  'validation_summary')
    inheritedAttributes.each { k -> flowFile.getAttribute(k)?.with { failureFlowFile = session.putAttribute(failureFlowFile, k, it) } }
    session.transfer(failureFlowFile, REL_FAILURE)
    log.info("Transferred ${invalidRecordsSummary.size()} validation errors to REL_FAILURE")
}
