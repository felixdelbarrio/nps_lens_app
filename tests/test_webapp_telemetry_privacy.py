import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_activity_details_keep_operations_and_discard_free_text() -> None:
    source = (ROOT / "webapp/apps-script/20_Activity.gs").read_text()
    subprocess.run(
        [
            "node",
            "-e",
            source
            + """
const assert = require('node:assert/strict');
for (const name of ['client_error', 'server_call', 'snapshot_load', 'view', 'app_enter']) {
  assert.ok(!_activityDetail_({name, detail:'Customer secret@example.com'}).includes('secret'));
}
assert.equal(_activityDetail_({name:'server_call', detail:'getPublishedShell'}), 'getPublishedShell');
assert.equal(_activityDetail_({name:'client_error', detail:'secret'}), 'client_error');
// Exercise the storage boundary, including a legacy client with raw error messages.
global._viewer_ = () => ({email:'admin@example.com'});
global._assertViewer_ = () => {};
global._property_ = () => 'configured';
global.NPS_LENS = {maxActivityBatch:50, maxActivityRows:50000, version:'test'};
global._cleanText_ = value => String(value || '');
global.LockService = {getScriptLock:() => ({waitLock:() => {}, releaseLock:() => {}})};
let stored;
global._sheet_ = () => ({getLastRow:() => 1, getRange:() => ({setValues:values => {stored=values;}})});
assert.equal(recordActivityEvents([{name:'client_error', detail:'Customer secret@example.com', durationMs:123}]).accepted, 1);
assert.equal(stored[0][7], 123);
assert.equal(stored[0][9], 'client_error');
""",
        ],
        check=True,
        capture_output=True,
        text=True,
    )


def test_browser_does_not_send_exception_messages() -> None:
    source = (ROOT / "webapp/apps-script/App.html").read_text()
    listener = source.split("window.addEventListener('error',", 1)[1].split("\n", 1)[0]
    subprocess.run(
        [
            "node",
            "-e",
            """
const assert = require('node:assert/strict');
let sent;
const queueActivity = (...args) => {sent=args;};
const window = {addEventListener:(name, callback) => callback({message:'Customer secret@example.com'})};
window.addEventListener('error',"""
            + listener
            + """
assert.deepEqual(sent, ['client_error', 'client_error', 'error']);
""",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
