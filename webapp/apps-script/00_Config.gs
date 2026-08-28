const NPS_LENS = Object.freeze({
  version: '2.3.1',
  domain: 'bbva.com',
  initialAdmin: 'felix.delbarrio@bbva.com',
  editionFileProperty: 'NPS_LENS_EDITION_FILE_ID',
  reportFileProperty: 'NPS_LENS_REPORT_FILE_ID',
  generatedAtProperty: 'NPS_LENS_GENERATED_AT',
  newsletterInsightProperty: 'NPS_LENS_NEWSLETTER_INSIGHT',
  publicationFolderProperty: 'NPS_LENS_PUBLICATION_FOLDER_ID',
  adminEmailsProperty: 'NPS_LENS_ADMIN_EMAILS',
  activitySheet: 'ACTIVIDAD_NPS_LENS',
  recipientsSheet: 'DESTINATARIOS_NEWSLETTER',
  maxPublicationBytes: 30 * 1024 * 1024,
  maxActivityRows: 50000,
  maxActivityBatch: 50
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
    .split(',').map(value => value.trim().toLowerCase())
    .filter(value => value.endsWith('@' + NPS_LENS.domain));
  const configuredAdmin = Boolean(email && admins.indexOf(email) >= 0);
  const initialAdmin = Boolean(email && email === NPS_LENS.initialAdmin);
  return {
    email,
    isAdmin: configuredAdmin || initialAdmin,
    role: configuredAdmin || initialAdmin ? 'admin' : 'viewer',
    adminSource: configuredAdmin ? 'configured' : initialAdmin ? 'initial-admin' : '',
    configurationReady: Boolean(_property_('NPS_LENS_SPREADSHEET_ID') && admins.length),
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

function _spreadsheet_() {
  const spreadsheetId = _property_('NPS_LENS_SPREADSHEET_ID');
  if (!spreadsheetId) throw new Error('Ejecuta setupNpsLensWebApp antes de utilizar la administración.');
  return SpreadsheetApp.openById(spreadsheetId);
}

function _sheet_(name) {
  const sheet = _spreadsheet_().getSheetByName(name);
  if (!sheet) throw new Error('Ejecuta setupNpsLensWebApp para preparar la sección ' + name + '.');
  return sheet;
}

function _cleanText_(value, limit) {
  return String(value == null ? '' : value).replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, limit || 500);
}
