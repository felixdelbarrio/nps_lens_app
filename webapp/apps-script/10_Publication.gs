function _publishedEdition_() {
  const fileId = _property_(NPS_LENS.editionFileProperty);
  if (!fileId) return {schema_version: '1.0', generated_at: '', screens: {}, manifest: {status: 'Sin edición publicada'}};
  const content = DriveApp.getFileById(fileId).getBlob().getDataAsString('UTF-8');
  const payload = JSON.parse(content);
  if (!payload || !payload.screens || !payload.manifest) throw new Error('La edición publicada no cumple el contrato NPS Lens.');
  return payload;
}

function getPublishedEdition() {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  return _publishedEdition_();
}

function setPublishedEditionFiles(payload) {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const editionFileId = String(payload && payload.editionFileId || '').trim();
  const reportFileId = String(payload && payload.reportFileId || '').trim();
  if (!editionFileId) throw new Error('Selecciona el JSON de la edición que se publicará.');
  const candidate = JSON.parse(DriveApp.getFileById(editionFileId).getBlob().getDataAsString('UTF-8'));
  if (!candidate.screens || !candidate.manifest) throw new Error('El fichero no cumple el contrato NPS Lens.');
  PropertiesService.getScriptProperties().setProperties({
    [NPS_LENS.editionFileProperty]: editionFileId,
    [NPS_LENS.reportFileProperty]: reportFileId
  });
  return getAdministration();
}

function getReportUrl() {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const fileId = _property_(NPS_LENS.reportFileProperty);
  return fileId ? DriveApp.getFileById(fileId).getUrl() : '';
}
