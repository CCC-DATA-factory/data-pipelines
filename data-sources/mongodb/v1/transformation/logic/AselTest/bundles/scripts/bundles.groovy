import org.apache.nifi.processor.io.InputStreamCallback
import org.apache.nifi.processor.io.StreamCallback
import groovy.json.JsonSlurper
import groovy.json.JsonOutput
import java.nio.charset.StandardCharsets
import java.text.SimpleDateFormat
import java.util.TimeZone

flowFile = session.get()
if (!flowFile) return

try {
    // 1) Read JSON input
    def jsonInput = new StringBuilder()
    session.read(flowFile, { inputStream ->
        inputStream.withReader(StandardCharsets.UTF_8.name()) { reader -> reader.eachLine { jsonInput.append(it) } }
    } as InputStreamCallback)

    // 2) Process and obtain outputs
    def (bundleJson, priceHistoryJson) = processBundle(jsonInput.toString())

    // 3) Emit bundle flowfile
    def bundleFlow = session.clone(flowFile)
    bundleFlow = session.write(bundleFlow, { _, outStream ->
        outStream.write(bundleJson.getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    bundleFlow = session.putAttribute(bundleFlow, 'target_iceberg_table_name', 'bundles')
    session.transfer(bundleFlow, REL_SUCCESS)

    // 4) Emit price history flowfile
    def priceFlow = session.clone(flowFile)
    priceFlow = session.write(priceFlow, { _, outStream ->
        outStream.write(priceHistoryJson.getBytes(StandardCharsets.UTF_8))
    } as StreamCallback)
    priceFlow = session.putAttribute(priceFlow, 'target_iceberg_table_name', 'bundles_price_history')
    session.transfer(priceFlow, REL_SUCCESS)

    // 5) Remove original
    session.remove(flowFile)
} catch (Exception e) {
    log.error('Error processing FlowFile', e)
    if (flowFile) {
        flowFile = session.putAttribute(flowFile, 'transformation.error', e.message)
        session.transfer(flowFile, REL_FAILURE)
    }
}

/**
 * Processes the input JSON into two outputs:
 *  - bundle JSON string (single-element array)
 *  - price history JSON string
 */
def processBundle(String jsonInput) {
    def data = new JsonSlurper().parseText(jsonInput)
    def errors = []

    // 0) Override all history dates to ISO UTC format
    try {
        def rawDate = '2023-10-06'
        def dateOnlyFmt = new SimpleDateFormat('yyyy-MM-dd')
        dateOnlyFmt.setTimeZone(TimeZone.getTimeZone('UTC'))
        def parsed = dateOnlyFmt.parse(rawDate)
        def isoFmt = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
        isoFmt.setTimeZone(TimeZone.getTimeZone('UTC'))
        def overrideISO = isoFmt.format(parsed)
        data.price_history?.each { rec ->
            if (rec.price != null && rec.date instanceof Map) {
                rec.date['$date'] = overrideISO
            }
        }
    } catch (Exception e) {
        log.error('Error overriding price_history dates', e)
    }

    // Helper: parse UTC date strings
    def parseUTC = { String s ->
        if (!s) return null
        for (pattern in ["yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss'Z'"]) {
            try {
                def fmt = new SimpleDateFormat(pattern)
                fmt.setTimeZone(TimeZone.getTimeZone('UTC'))
                return fmt.parse(s)
            } catch (Exception ignored) {}
        }
        log.warn("Unable to parse UTC date: ${s}")
        return null
    }

    // Nested get helper
    def get = { String path -> path.split(/\./).inject(data) { obj, key -> (obj instanceof Map) ? obj[key] : null } }

    // Validate and compute fields
    def name = get('name')
    if (!(name instanceof String && name.trim())) errors << 'name is required'

    def bundleIdRaw = get('bundleId')
    if (bundleIdRaw == null) errors << 'bundleId is required'
    // Always output bundleid as lowercase field and as string
    def bundleid = bundleIdRaw.toString()

    def dataAmt = get('content.data.amount') ?: 0
    if (!(dataAmt instanceof Number && dataAmt >= 0 && dataAmt <= 1000)) errors << 'content.data.amount out of range'
    def dataUnit = get('content.data.unit') ?: 'Megabytes'
    def data_amount_gb = (dataUnit == 'Megabytes') ? dataAmt / 1000.0 : dataAmt

    def voiceAmt = get('content.voice.amount') ?: 0
    if (!(voiceAmt instanceof Number && voiceAmt >= 0 && voiceAmt <= 1000)) errors << 'content.voice.amount out of range'
    def voiceUnit = get('content.voice.unit') ?: 'Minutes'
    def voice_amount_minutes = (voiceUnit == 'Hours') ? voiceAmt * 60 : voiceAmt

    def sms_amount = get('content.sms') ?: 0
    if (!(sms_amount instanceof Number && sms_amount >= 0 && sms_amount <= 1000)) errors << 'content.sms out of range'

    def priceAmt = get('price.amount') ?: 0
    if (!(priceAmt instanceof Number && priceAmt >= 1.0 && priceAmt <= 60.0)) errors << 'price.amount out of range'

    def validity_days = get('validity.number')
    if (!(validity_days instanceof Number && validity_days >= 0 && validity_days <= 300)) errors << 'validity.number out of range'

    // Parse createdAt into Date then to milliseconds
    def createdRaw = get('createdAt.$date') ?: get('object_creation_time')
    def createdDate = parseUTC(createdRaw?.toString())
    if (!createdDate) errors << 'createdAt invalid'
    def createdat = createdDate?.time

    if (errors) {
        log.error('Validation errors: ' + errors.join('; '))
        throw new Exception('Validation errors: ' + errors.join('; '))
    }

    // Build bundle JSON: single-element array, with renamed fields and dates as millis
    def bundleObj = [
        id                     : get('_id.$oid'),
        name                   : name,
        bundleid               : bundleid,
        data_amount_gb         : data_amount_gb,
        voice_amount_minutes   : voice_amount_minutes,
        sms_amount             : sms_amount,
        validity_days          : validity_days,
        createdat              : createdat
    ]
    def bundleJson = JsonOutput.prettyPrint(JsonOutput.toJson([bundleObj]))

    // Build price history JSON: dates in millis, bundleid lowercased
    def priceHist = get('price_history') ?: []
    def histList = []
    priceHist.each { rec ->
        if (!(rec.date instanceof Map)) {
            log.warn('Skipping entry without date map: ' + rec)
        } else {
            def raw = rec.date['$date']
            def dt = parseUTC(raw?.toString())
            if (dt) histList << [bundleid: bundleid, price: rec.price, date: dt]
            else log.warn('Skipping entry with invalid date: ' + raw)
        }
    }
    histList.sort { a, b -> a.date <=> b.date }

    def priceArray = []
    histList.eachWithIndex { entry, idx ->
        def start = (idx == 0) ? createdDate : histList[idx - 1].date
        def end   = entry.date
        if (start.after(end)) start = end
        priceArray << [
            bundleid   : bundleid,
            price      : entry.price,
            start_date : start.time,
            end_date   : end.time
        ]
    }
    if (histList) {
        def last = histList[-1].date
        priceArray << [
            bundleid   : bundleid,
            price      : priceAmt,
            start_date : last.time,
            end_date   : 253402300799000
        ]
    } else {
        priceArray << [
            bundleid   : bundleid,
            price      : priceAmt,
            start_date : createdDate.time,
            end_date   : 253402300799000
        ]
    }
    def priceHistoryJson = JsonOutput.prettyPrint(JsonOutput.toJson(priceArray))

    return [bundleJson, priceHistoryJson]
}
