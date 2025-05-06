import groovy.lang.Binding
import groovy.lang.GroovyShell
import org.apache.nifi.flowfile.FlowFile
import java.io.File

// 1) Fetch exactly one FlowFile
FlowFile flowFile = session.get()
if (flowFile == null) return  // nothing to do

// 2) Resolve your relationships
def REL_SUCCESS = context.getAvailableRelationships().find { it.getName() == 'success' }
def REL_FAILURE = context.getAvailableRelationships().find { it.getName() == 'failure' }

try {
    // 3) Normalize and load the external script
    def rawPath = flowFile.getAttribute('script-path')
    if (!rawPath) throw new Exception("Missing script-path attribute")
    def scriptPath = rawPath.replace("\\", "/")  // win→unix
    def scriptFile = new File(scriptPath)
    if (!scriptFile.exists() || !scriptFile.canRead()) {
        throw new Exception("Cannot read script at ${scriptPath}")
    }
    def scriptText = scriptFile.getText('UTF-8')

    // 4) Bind NiFi objects for the external Groovy
    def binding = new Binding([
        flowFile    : flowFile,
        session     : session,
        log         : log,
        REL_SUCCESS : REL_SUCCESS,
        REL_FAILURE : REL_FAILURE
    ])

    // 5) Execute the external script (it must clone & transfer two outputs)
    new GroovyShell(binding).evaluate(scriptText)

    // 6) Remove the original FlowFile (all work is done on its clones)
    session.remove(flowFile)

} catch (Exception e) {
    // 7) On any error, attach the message and route to failure
    log.error("Script failure for ${flowFile.getAttribute('script-path')}: ${e.message}", e)
    flowFile = session.putAttribute(flowFile, 'script.error', e.message)
    session.transfer(flowFile, REL_FAILURE)
}
