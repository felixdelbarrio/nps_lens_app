function onOpen() {
  SpreadsheetApp.getUi().createMenu('NPS Lens')
    .addItem('Validar configuración', 'getAdministration')
    .addItem('Diagnosticar acceso', 'diagnoseNpsLensAccess')
    .addItem('Preparar administración', 'setupFromActiveSpreadsheet')
    .addItem('Borrar cachés e informes', 'clearNpsLensCachesAndReports')
    .addToUi();
}

function setupFromActiveSpreadsheet() {
  const active = SpreadsheetApp.getActiveSpreadsheet();
  return setupNpsLensWebApp(active ? active.getId() : '', '');
}

function clearNpsLensCachesAndReports() {
  const viewer = _viewer_(); _assertAdmin_(viewer);
  const lock = LockService.getScriptLock(); lock.waitLock(30000);
  try {
    const rows = _publicationRows_(), properties = PropertiesService.getScriptProperties(), stored = properties.getProperties();
    const ids = Array.from(new Set(rows.flatMap(item => [item.snapshotFileId,item.pptxFileId,item.slidesFileId,
      stored[_publicationShellProperty_(item.scopeKey)]]).filter(Boolean)));
    const failed = [];
    ids.forEach(id => { try { DriveApp.getFileById(id).setTrashed(true); } catch (error) { failed.push(id); } });
    if (failed.length) throw new Error('No se han podido enviar a la papelera ' + failed.length + ' ficheros.');
    const sheet = _sheet_(NPS_LENS.publicationsSheet), lastRow = sheet.getLastRow();
    if (lastRow > 1) sheet.getRange(2,1,lastRow-1,PUBLICATION_HEADERS.length).clearContent();
    rows.forEach(item => properties.deleteProperty(_publicationShellProperty_(item.scopeKey)));
    properties.deleteProperty(NPS_LENS.selectedScopeProperty);
    NPS_LENS_REQUEST.cacheEpoch = String(Date.now());
    properties.setProperty(NPS_LENS.cacheEpochProperty, NPS_LENS_REQUEST.cacheEpoch);
    publicationRowsCache = [];
    return {ok:true,filesTrashed:ids.length,publicationsDeleted:rows.length};
  } finally { lock.releaseLock(); }
}
