"""Verify persisted visibility through the actual newsletter MIME and Slides selection."""

import subprocess
from pathlib import Path


def test_saved_visibility_controls_newsletter_content_and_presentation():
    root = Path("webapp/apps-script")
    source = "\n".join(
        (root / name).read_text()
        for name in ("00_Brand.gs", "10_Publication.gs", "30_Administration.gs", "40_Newsletter.gs")
    )
    script = (
        r"""
const assert = require('node:assert/strict');
const NPS_LENS = {version:'test',evolutionNpsVisibleProperty:'visible',selectedScopeProperty:'selected',newsletterSenderName:'NPS Lens'};
const properties = new Map([['selected','edition']]);
function _property_(key){return properties.get(key)||'';}
function _viewer_(){return {email:'admin@bbva.com',isAdmin:true};}
function _assertAdmin_(viewer){assert.equal(viewer.isAdmin,true);}
const PropertiesService = {getScriptProperties:()=>({setProperty:(key,value)=>properties.set(key,value)})};
const ScriptApp = {getService:()=>({getUrl:()=> 'https://script.google.com/test/exec'})};
const Utilities = {
  Charset:{UTF_8:'utf8'}, getUuid:()=> 'test-message',
  base64Encode:value=>Buffer.from(value).toString('base64'),
  base64EncodeWebSafe:value=>Buffer.from(value).toString('base64url')
};
let delivered;
const Gmail = {Users:{Messages:{send:message=>{
  const mime=Buffer.from(message.raw,'base64url').toString();
  const subject=Buffer.from(mime.match(/Subject: =\?UTF-8\?B\?([^?]+)\?=/)[1],'base64').toString();
  assert.ok(subject.startsWith('[bIA]'));
  delivered=[...mime.matchAll(/Content-Transfer-Encoding: base64\r\n\r\n([\s\S]*?)\r\n--/g)]
    .map(match=>Buffer.from(match[1],'base64').toString());
  return {id:'accepted'};
}}}};
"""
        + source
        + r"""
_newsletterSenderIdentity_=()=>({ready:true,effective:'sender@bbva.com'});
const insight={brand:'BBVA',product:'NPS Lens',promise:'Escucha del cliente',period:'Agosto',
  headline:'Titular con NPS',lead:'Balance de NPS del periodo',
  scorecard:[{label:'NPS clásico mensual',value:'17,42',delta:'-0,68'}],
  quotes:['La página es lenta'],signals:[{label:'Continuidad',reason:'Incidencias recurrentes'},{label:'Tema cliente',reason:'NPS -94,67; 94,67% detractores; 150 opiniones.'}]};
publicationRowsCache=[{scopeKey:'edition',slidesFileId:'full',newsletterInsight:JSON.stringify(insight)}];
properties.set(_compactSlidesProperty_('edition'),'compact');
const original=JSON.stringify(insight);
for(const visible of [false,true,false]){
  const saved=saveEvolutionNpsSettings(visible,'edition');
  assert.equal(getEvolutionNpsSettings().visible,visible);
  const report=visible?'full':'compact';
  assert.equal(saved.reportUrl,'https://docs.google.com/presentation/d/'+report+'/edit');
  const sent=testNewsletter();
  assert.equal(sent.presentationUrl,saved.reportUrl);
  assert.equal(delivered.length,2);
  for(const content of delivered){
    assert.equal(content.includes(insight.headline),visible);
    assert.equal(content.includes(insight.lead),visible);
    assert.equal(content.includes('NPS clásico mensual'),visible);
    assert.equal(content.includes('17,42'),visible);
    for(const signal of insight.signals){
      assert.ok(content.includes(signal.label));
      assert.ok(content.includes(signal.reason));
    }
    assert.ok(content.includes('bIA'));
    assert.ok(content.includes('Banca Inteligente y Autónoma'));
    assert.ok(content.includes('La página es lenta'));
    assert.ok(content.includes('Incidencias recurrentes'));
    assert.ok(content.includes(saved.reportUrl));
    assert.ok(content.includes('?source=newsletter&scope=edition'));
  }
  assert.equal(delivered[0].includes('INDICADORES'),visible);
  assert.equal(delivered[1].includes('<table class="metric-table"'),visible);
}
assert.equal(JSON.stringify(insight),original);
"""
    )
    subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
