function doGet(event) {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const requestedScope = String(event && event.parameter && event.parameter.scope || '').trim();
  const publication = _publicationByKey_(requestedScope || _property_(NPS_LENS.selectedScopeProperty));
  viewer.scopeKey = publication ? publication.scopeKey : '';
  viewer.reportUrl = _reportUrl_(viewer.scopeKey);
  if (viewer.isAdmin) {
    viewer.publicationCatalog = _publicationCatalog_();
    viewer.selectedScopeKey = (_selectedPublication_() || {}).scopeKey || '';
    viewer.administration = {version:NPS_LENS.version,generatedAt:publication ? publication.generatedAt : '',access:{
      email:viewer.email,role:viewer.role,source:viewer.adminSource,configurationReady:viewer.configurationReady}};
  }
  const template = HtmlService.createTemplateFromFile('Index');
  const edition = _publishedShell_(viewer.scopeKey);
  template.publicationJson = JSON.stringify(edition).replace(/<\//g, '<\\/');
  template.viewerJson = JSON.stringify(viewer);
  template.adminBodyClass = viewer.isAdmin ? 'is-admin' : '';
  template.accessRole = viewer.role;
  template.appVersion = NPS_LENS.version;
  return template.evaluate()
    .setTitle('NPS Lens · BBVA Banca de Empresas e Instituciones')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function getPublishedDataset(datasetKind, scopeKey, scoreChannel, npsGroup) {
  const viewer = _viewer_(); _assertViewer_(viewer);
  const kind = String(datasetKind || '').toLowerCase();
  if (['nps','helix'].indexOf(kind) < 0) throw new Error('Base de datos no soportada.');
  const publication = _publicationByKey_(scopeKey);
  if (!publication) throw new Error('El ámbito solicitado no está publicado.');
  const source = (_loadSnapshot_(publication.snapshotFileId).datasets || {})[kind];
  if (!source) throw new Error('La edición debe volver a publicarse para generar sus snapshots de datos.');
  const page = kind === 'helix' ? source.page : source.pages[
    _normalizedDatasetFilter_(scoreChannel) + '|' + _normalizedDatasetFilter_(npsGroup)];
  if (!page) return {dataset_kind:kind,total_rows:0,columns:source.columns || [],rows:[]};
  return {dataset_kind:kind,total_rows:page.total_rows,columns:source.columns || [],rows:page.rows || []};
}
