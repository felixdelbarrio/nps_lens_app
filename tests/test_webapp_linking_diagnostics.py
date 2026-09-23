"""Execute the static renderer against both current and legacy publications."""

import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_linking_diagnostics_render_empty_results_and_escape_scope() -> None:
    source = (ROOT / "webapp/apps-script/App.html").read_text()
    helpers = source.split("  const rows =", 1)[1].split("  const summaryViews", 1)[0]
    functions = []
    for name in ["table", "evidenceText", "linkingDiagnostics", "linkingView"]:
        function = "  function " + source.split("  function " + name + "(", 1)[1]
        function = function.replace("  function ", "  function " + name + "(", 1)
        functions.append(function.split("\n  function ", 1)[0])
    subprocess.run(
        [
            "node",
            "-e",
            "const rows ="
            + helpers
            + "\n".join(functions)
            + """
const assert = require('node:assert/strict');
assert.equal(linkingDiagnostics(undefined), '');
assert.equal(linkingDiagnostics({}), '');
const diagnostics = {nps_total:100, nps_focus:20, nps_matchable:15,
  nps_non_matchable:5, linked_incidents:0, nps_coverage_pct:0,
  scope_requested_n1_n2:['<img src=x onerror=alert(1)>'],
  exclusions:{non_matchable:5}, principal_exclusion_reason:'non_matchable'};
const html = linkingDiagnostics(diagnostics);
assert.ok(html.includes('Respuestas no analizables'));
assert.ok(html.includes('<td>5</td>'));
assert.ok(html.includes('<td>0</td>'));
assert.ok(html.includes('&lt;img'));
assert.ok(!html.includes('<img'));
assert.ok(html.includes('Sin comentario ni taxonomía útil'));
const linkingViews = [];
const contentTabs = () => '';
const hero = (title, copy, options) => title + copy + options.body;
const causalMethodFilter = () => 'Recorridos rotos';
let linking = {available:false, diagnostics};
const empty = linkingView('situation');
assert.ok(empty.includes('Diagnóstico del cruce'));
assert.ok(empty.includes('Recorridos rotos'));
const heading = () => '';
const metrics = () => '';
const topicFilter = () => '';
const state = {evidenceTopic:''};
linking = {available:true, diagnostics};
assert.ok(linkingView('situation').includes('Diagnóstico del cruce'));
linking = {available:true};
assert.ok(!linkingView('situation').includes('Diagnóstico del cruce'));
linking = {available:false};
assert.ok(!linkingView('situation').includes('Diagnóstico del cruce'));
""",
        ],
        check=True,
        capture_output=True,
        text=True,
    )
