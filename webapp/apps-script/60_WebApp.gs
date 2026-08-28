function doGet(event) {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const requestedScope = String(event && event.parameter && event.parameter.scope || '').trim();
  const publication = _publicationByKey_(requestedScope || _property_(NPS_LENS.selectedScopeProperty));
  viewer.scopeKey = publication ? publication.scopeKey : '';
  viewer.reportUrl = _reportUrl_(viewer.scopeKey);
  const template = HtmlService.createTemplateFromFile('Index');
  const edition = _publishedEdition_(viewer.scopeKey);
  const data = edition.screens && edition.screens.data || {};
  Object.keys(data).forEach(kind => { if (data[kind] && Array.isArray(data[kind].rows)) {
    data[kind].rows = []; data[kind].deferred = true;
  }});
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

function getPublishedDataset(datasetKind, scopeKey, offset, limit, scoreChannel, npsGroup) {
  const viewer = _viewer_(); _assertViewer_(viewer);
  const kind = String(datasetKind || '').toLowerCase();
  if (['nps','helix'].indexOf(kind) < 0) throw new Error('Base de datos no soportada.');
  const edition = _publishedEdition_(scopeKey);
  const source = edition.screens && edition.screens.data && edition.screens.data[kind] || {};
  let rows = Array.isArray(source.rows) ? source.rows : [];
  const channel = String(scoreChannel || 'Todos').trim().toLowerCase();
  const group = String(npsGroup || 'Todos').trim().toLowerCase();
  if (kind === 'nps' && channel !== 'todos') rows = rows.filter(row => String(row.Canal || '').trim().toLowerCase() === channel);
  if (kind === 'nps' && group !== 'todos') rows = rows.filter(row => String(row['NPS Group'] || '').trim().toLowerCase() === group.replace(/^detractores$/,'detractor').replace(/^promotores$/,'promotor').replace(/^neutros$/,'pasivo'));
  const start = Math.max(0, Number(offset) || 0), pageSize = Math.min(500, Math.max(1, Number(limit) || 250));
  return {dataset_kind:kind,total_rows:rows.length,offset:start,limit:pageSize,columns:source.columns || [],rows:rows.slice(start,start+pageSize),has_more:start+pageSize<rows.length};
}
