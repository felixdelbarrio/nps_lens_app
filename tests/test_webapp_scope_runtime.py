"""Execute Apps Script routing with an in-memory publication registry."""

import subprocess
from pathlib import Path


def test_snapshot_navigation_and_newsletter_are_isolated():
    root = Path("webapp/apps-script")
    source = (root / "10_Publication.gs").read_text() + (root / "60_WebApp.gs").read_text()
    script = (
        r"""
const assert = require('node:assert/strict');
let selected = 'ar-old';
const NPS_LENS = {selectedScopeProperty:'selected',version:'test'};
let currentViewer;
function _viewer_(){ return currentViewer; }
function _assertViewer_(){}
function _property_(key){return key==='selected'?selected:'';}
function _evolutionNpsVisible_(){return true;}
const HtmlService = {
  XFrameOptionsMode:{ALLOWALL:1},
  createTemplateFromFile(){return {evaluate(){
    const result = {viewer:JSON.parse(this.viewerJson),publication:JSON.parse(this.publicationJson)};
    result.setTitle=result.setXFrameOptionsMode=result.addMetaTag=()=>result;
    return result;
  }};}
};
"""
        + source
        + r"""
publicationRowsCache = [
 {scopeKey:'ar-old',audienceKey:'ar',ownerSupportCompany:'Argentina',generatedAt:'2026-08-01',importedAt:'2026-10-01',slidesFileId:'old-report'},
 {scopeKey:'mx',audienceKey:'mx',ownerSupportCompany:'México',generatedAt:'2026-09-01',importedAt:'2026-09-01',slidesFileId:'mx-report'},
 {scopeKey:'ar-new',audienceKey:'ar',ownerSupportCompany:'Argentina',generatedAt:'2026-09-30',importedAt:'2026-09-30',slidesFileId:'new-report'},
];
const initial = JSON.stringify(publicationRowsCache);
function open(parameters, admin=false){
  currentViewer={isAdmin:admin,role:admin?'admin':'viewer'};
  return doGet({parameter:parameters});
}
assert.equal(_publicationByKey_('missing'),null);
assert.equal(_entryPublication_('ar-old',false).scopeKey,'ar-new');
assert.equal(_entryPublication_('ar-old',true).scopeKey,'ar-old');
assert.throws(()=>_entryPublication_('missing',true),/no está publicado/);
assert.throws(()=>_entryPublication_('',true),/no identifica/);
for (const admin of [false,true]) {
  const direct=open({},admin);
  assert.equal(direct.viewer.scopeKey,'ar-new');
  assert.deepEqual(direct.viewer.publicationCatalog.map(x=>x.scopeKey),['ar-new','mx']);
  assert.equal(open({scope:'mx'},admin).viewer.scopeKey,'mx');
  assert.equal(open({scope:'ar-old'},admin).viewer.scopeKey,'ar-new');
  const newsletter=open({scope:'ar-old',source:'newsletter'},admin);
  assert.equal(newsletter.viewer.scopeKey,'ar-old');
  assert.equal(newsletter.viewer.reportUrl,'https://docs.google.com/presentation/d/old-report/edit');
  assert.equal(newsletter.viewer.isAdmin,false);
  assert.deepEqual(newsletter.viewer.publicationCatalog,[]);
}
assert.equal(JSON.stringify(publicationRowsCache),initial);
assert.throws(()=>getPublishedShell('missing',false),/no está publicado/);
assert.throws(()=>getPublishedDataset('nps','missing'),/no está publicado/);
// Import into a Drive/Sheet test double: existing editions and presentations survive.
const properties = new Map([['selected','ar-old']]);
const created = [], trashed = [], appended = [];
_property_ = key => properties.get(key)||'';
_clearPublicationCache_ = () => {};
_publicationFolder_ = () => ({id:'destination'});
_newsletterInsight_ = () => ({});
_administration_ = value => value;
_cacheEncodedSnapshot_ = () => {};
function _assertAdmin_(){}
const PropertiesService = {getScriptProperties:()=>({
 setProperties:values=>Object.entries(values).forEach(([key,value])=>properties.set(key,value)),
 setProperty:(key,value)=>properties.set(key,value),deleteProperty:key=>properties.delete(key)
})};
let locks=0;
const LockService = {getScriptLock:()=>({waitLock:()=>locks++,releaseLock:()=>locks--})};
function _sheet_(){return {appendRow:row=>appended.push(row)};}
let failConversion=false;
const Drive={Files:{create:(metadata,blob)=>{
 if(failConversion && metadata.mimeType==='application/vnd.google-apps.presentation')throw Error('conversion failed');
 const id='file-'+created.length;created.push(id);return {id};
}}};
const DriveApp={getFileById:id=>({setTrashed:()=>trashed.push(id)})};
function blob(name,text='payload'){return {getName:()=>name,getBytes:()=>[1,2,3],getDataAsString:()=>text};}
const edition={schema_version:'5.0',generated_at:'2026-10-01',
 scope:{key:'ar-third',audience_key:'ar',owner_support_company:'Argentina',year:'2026',month:'08',causal_method:'domain_touchpoint',causal_method_label:'Subpalanca'},
 screens:{dashboard:{},linking:{},data:{}},manifest:{report:'full.pptx',report_without_evolution:'compact.pptx'},
 snapshots:{data:{schema_version:'5.0',datasets:{nps:{page:{}},helix:{page:{}}}}}};
const Utilities={newBlob:(bytes,mime,name)=>blob(name),gzip:value=>value,base64EncodeWebSafe:()=>'',
 unzip:()=>[blob('publication.json',JSON.stringify(edition)),blob('full.pptx'),blob('compact.pptx')]};
_importPublicationArchiveBlob_(blob('edition.zip'));
assert.equal(locks,0);
assert.equal(appended.length,1);
assert.equal(appended[0][0],'ar-third');
assert.equal(created.length,6);
assert.deepEqual(trashed,[]);
assert.equal(JSON.stringify(publicationRowsCache),initial);
assert.equal(_reportUrl_('ar-old',true),'https://docs.google.com/presentation/d/old-report/edit');
assert.equal(_reportUrl_('mx',true),'https://docs.google.com/presentation/d/mx-report/edit');
const committedProperties = [...properties.entries()];
failConversion=true;edition.scope.key='ar-failed';
assert.throws(()=>_importPublicationArchiveBlob_(blob('failed.zip')),/conversion failed/);
assert.equal(locks,0);
assert.equal(appended.length,1);
assert.deepEqual([...properties.entries()],committedProperties);
assert.deepEqual(trashed,created.slice(6));
failConversion=false;edition.scope.key='ar-old';
assert.throws(()=>_importPublicationArchiveBlob_(blob('duplicate.zip')),/ya está publicada/);
assert.equal(appended.length,1);

"""
    )
    subprocess.run(["node", "-e", script], check=True, capture_output=True, text=True)
