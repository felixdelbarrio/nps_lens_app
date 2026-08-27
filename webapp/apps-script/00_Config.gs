const NPS_LENS = Object.freeze({
  version: '2.1.0',
  domain: 'bbva.com',
  editionFileProperty: 'NPS_LENS_EDITION_FILE_ID',
  reportFileProperty: 'NPS_LENS_REPORT_FILE_ID',
  publicationFolderProperty: 'NPS_LENS_PUBLICATION_FOLDER_ID',
  adminEmailsProperty: 'NPS_LENS_ADMIN_EMAILS',
  telemetrySheet: 'TELEMETRIA_NPS_LENS',
  maxPublicationBytes: 30 * 1024 * 1024,
  maxTelemetryRows: 10000
});

function include(name) {
  return HtmlService.createHtmlOutputFromFile(name).getContent();
}

function _property_(key) {
  return String(PropertiesService.getScriptProperties().getProperty(key) || '').trim();
}

function _viewer_() {
  const email = String(Session.getActiveUser().getEmail() || '').trim().toLowerCase();
  const admins = _property_(NPS_LENS.adminEmailsProperty)
    .split(',').map(value => value.trim().toLowerCase()).filter(Boolean);
  return {
    email,
    isAdmin: Boolean(email && admins.indexOf(email) >= 0),
    domainAllowed: Boolean(email && email.endsWith('@' + NPS_LENS.domain))
  };
}

function _assertViewer_(viewer) {
  if (!viewer.domainAllowed) throw new Error('Esta edición está disponible únicamente para el dominio BBVA.');
}

function _assertAdmin_(viewer) {
  _assertViewer_(viewer);
  if (!viewer.isAdmin) throw new Error('Esta operación está reservada a administradores.');
}
