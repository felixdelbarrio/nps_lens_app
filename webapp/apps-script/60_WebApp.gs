function doGet(event) {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const requestedScope = String(event && event.parameter && event.parameter.scope || '').trim();
  const publication = _publicationByKey_(requestedScope || _property_(NPS_LENS.selectedScopeProperty));
  viewer.scopeKey = publication ? publication.scopeKey : '';
  viewer.evolutionNpsVisible = _evolutionNpsVisible_();
  viewer.reportUrl = publication ? _reportUrl_(publication.scopeKey,viewer.evolutionNpsVisible) : '';
  viewer.causalMethodLocked = String(event && event.parameter && event.parameter.source || '') === 'newsletter';
  viewer.shellDeferred = Boolean(publication);
  viewer.publicationCatalog = _publicationCatalog_();
  if (viewer.isAdmin) {
    viewer.selectedScopeKey = (_selectedPublication_() || {}).scopeKey || '';
    viewer.administration = {version:NPS_LENS.version,generatedAt:publication ? publication.generatedAt : '',access:{
      email:viewer.email,role:viewer.role,source:viewer.adminSource,configurationReady:viewer.configurationReady}};
  }
  const template = HtmlService.createTemplateFromFile('Index');
  const bootstrap = {schema_version:'5.0',generated_at:publication ? publication.generatedAt : '',scope:{key:publication ? publication.scopeKey : ''},screens:{},static_views:{},filters:{}};
  template.publicationJson = JSON.stringify(bootstrap);
  template.viewerJson = JSON.stringify(viewer);
  template.adminBodyClass = viewer.isAdmin ? 'is-admin' : '';
  template.accessRole = viewer.role;
  template.appVersion = NPS_LENS.version;
  return template.evaluate()
    .setTitle('NPS Lens · BBVA Banca de Empresas e Instituciones')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}

function getPublishedShell(scopeKey, compressed) {
  const viewer = _viewer_(); _assertViewer_(viewer);
  const publication = _publicationByKey_(scopeKey);
  if (!publication) throw new Error('El ámbito solicitado no está publicado.');
  const fileId = _property_(_publicationShellProperty_(publication.scopeKey));
  if (!fileId) throw new Error('Esta edición debe volver a publicarse para aplicar la carga optimizada.');
  return {encoding:compressed ? 'gzip-base64' : 'json',payload:compressed ? _encodedSnapshot_(fileId) : _loadSnapshot_(fileId)};
}

function getPublishedDataset(datasetKind, scopeKey) {
  const viewer = _viewer_(); _assertViewer_(viewer);
  const kind = String(datasetKind || '').toLowerCase();
  if (['nps','helix'].indexOf(kind) < 0) throw new Error('Base de datos no soportada.');
  const publication = _publicationByKey_(scopeKey);
  if (!publication) throw new Error('El ámbito solicitado no está publicado.');
  const source = (_loadSnapshot_(publication.snapshotFileId).datasets || {})[kind];
  if (!source) throw new Error('La edición debe volver a publicarse para generar sus snapshots de datos.');
  const page = source.page;
  if (!page) return {dataset_kind:kind,total_rows:0,columns:source.columns || [],rows:[]};
  return {dataset_kind:kind,total_rows:page.total_rows,columns:source.columns || [],rows:page.rows || []};
}
