function doGet() {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const template = HtmlService.createTemplateFromFile('Index');
  template.publicationJson = JSON.stringify(_publishedEdition_()).replace(/<\//g, '<\\/');
  template.viewerJson = JSON.stringify(viewer);
  template.adminBodyClass = viewer.isAdmin ? 'is-admin' : '';
  template.accessRole = viewer.role;
  template.appVersion = NPS_LENS.version;
  return template.evaluate()
    .setTitle('NPS Lens · BBVA Banca de Empresas e Instituciones')
    .setXFrameOptionsMode(HtmlService.XFrameOptionsMode.ALLOWALL)
    .addMetaTag('viewport', 'width=device-width, initial-scale=1');
}
