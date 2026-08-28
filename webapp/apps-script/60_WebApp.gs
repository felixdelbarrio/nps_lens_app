function doGet(event) {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const requestedScope = String(event && event.parameter && event.parameter.scope || '').trim();
  const publication = _publicationByKey_(requestedScope || _property_(NPS_LENS.selectedScopeProperty));
  viewer.scopeKey = publication ? publication.scopeKey : '';
  viewer.reportUrl = _reportUrl_(viewer.scopeKey);
  const template = HtmlService.createTemplateFromFile('Index');
  template.publicationJson = JSON.stringify(_publishedEdition_(viewer.scopeKey)).replace(/<\//g, '<\\/');
  template.viewerJson = JSON.stringify(viewer);
  template.adminBodyClass = viewer.isAdmin ? 'is-admin' : '';
  template.accessRole = viewer.role;
  template.appVersion = NPS_LENS.version;
  return template.evaluate()
    .setTitle('NPS Lens · BBVA Banca de Empresas e Instituciones')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}
