const NPS_LENS = Object.freeze({
  version: '2.9.9',
  domain: 'bbva.com',
  initialAdmin: 'felix.delbarrio@bbva.com',
  newsletterFrom: 'nps-lens.group@bbva.com',
  newsletterSenderName: 'NPS Lens',
  publicationFolderProperty: 'NPS_LENS_PUBLICATION_FOLDER_ID',
  cacheEpochProperty: 'NPS_LENS_CACHE_EPOCH',
  selectedScopeProperty: 'NPS_LENS_SELECTED_SCOPE_KEY',
  evolutionNpsVisibleProperty: 'NPS_LENS_EVOLUTION_NPS_VISIBLE',
  adminEmailsProperty: 'NPS_LENS_ADMIN_EMAILS',
  activitySheet: 'ACTIVIDAD_NPS_LENS',
  recipientsSheet: 'DESTINATARIOS_NEWSLETTER',
  publicationsSheet: 'PUBLICACIONES_NPS',
  maxPublicationBytes: 30 * 1024 * 1024,
  maxActivityRows: 50000,
  maxActivityBatch: 50
});

const NPS_LENS_REQUEST = {viewer:null,spreadsheet:null,sheets:{},cacheEpoch:null};

function include(name) {
  return HtmlService.createHtmlOutputFromFile(name).getContent();
}

function _property_(key) {
  return String(PropertiesService.getScriptProperties().getProperty(key) || '').trim();
}

function _cacheKey_(name) {
  if (NPS_LENS_REQUEST.cacheEpoch === null) NPS_LENS_REQUEST.cacheEpoch = _property_(NPS_LENS.cacheEpochProperty);
  return ['nps-lens',NPS_LENS.version,NPS_LENS_REQUEST.cacheEpoch,name].join('-');
}

function _viewer_() {
  if (NPS_LENS_REQUEST.viewer) return NPS_LENS_REQUEST.viewer;
  const email = String(Session.getActiveUser().getEmail() || '').trim().toLowerCase();
  const admins = _property_(NPS_LENS.adminEmailsProperty)
    .split(',').map(value => value.trim().toLowerCase())
    .filter(value => value.endsWith('@' + NPS_LENS.domain));
  const configuredAdmin = Boolean(email && admins.indexOf(email) >= 0);
  const initialAdmin = Boolean(email && email === NPS_LENS.initialAdmin);
  NPS_LENS_REQUEST.viewer = {
    email,
    isAdmin: configuredAdmin || initialAdmin,
    role: configuredAdmin || initialAdmin ? 'admin' : 'viewer',
    adminSource: configuredAdmin ? 'configured' : initialAdmin ? 'initial-admin' : '',
    configurationReady: Boolean(_property_('NPS_LENS_SPREADSHEET_ID') && admins.length),
    domainAllowed: Boolean(email && email.endsWith('@' + NPS_LENS.domain))
  };
  return NPS_LENS_REQUEST.viewer;
}

function _assertViewer_(viewer) {
  if (!viewer.domainAllowed) throw new Error('Esta edición está disponible únicamente para el dominio BBVA.');
}

function _assertAdmin_(viewer) {
  _assertViewer_(viewer);
  if (!viewer.isAdmin) throw new Error('Esta operación está reservada a administradores.');
}

function _spreadsheet_() {
  if (NPS_LENS_REQUEST.spreadsheet) return NPS_LENS_REQUEST.spreadsheet;
  const spreadsheetId = _property_('NPS_LENS_SPREADSHEET_ID');
  if (!spreadsheetId) throw new Error('Ejecuta setupNpsLensWebApp antes de utilizar la administración.');
  NPS_LENS_REQUEST.spreadsheet = SpreadsheetApp.openById(spreadsheetId);
  return NPS_LENS_REQUEST.spreadsheet;
}

function _sheet_(name) {
  if (NPS_LENS_REQUEST.sheets[name]) return NPS_LENS_REQUEST.sheets[name];
  const sheet = _spreadsheet_().getSheetByName(name);
  if (!sheet) throw new Error('Ejecuta setupNpsLensWebApp para preparar la sección ' + name + '.');
  NPS_LENS_REQUEST.sheets[name] = sheet;
  return sheet;
}

function _cleanText_(value, limit) {
  return String(value == null ? '' : value).replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, limit || 500);
}
