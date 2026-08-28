function getAdministration() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const edition = _publishedEdition_();
  return {
    version: NPS_LENS.version,
    generatedAt: edition.generated_at || '',
    reportUrl: getReportUrl(),
    access: {
      email: viewer.email,
      role: viewer.role,
      source: viewer.adminSource,
      configurationReady: viewer.configurationReady
    }
  };
}

function diagnoseNpsLensAccess() {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  return {
    version: NPS_LENS.version,
    email: viewer.email,
    role: viewer.role,
    adminSource: viewer.adminSource,
    configurationReady: viewer.configurationReady,
    webAppUrl: ScriptApp.getService().getUrl()
  };
}

function setupNpsLensWebApp(spreadsheetId, adminEmails) {
  const viewer = _viewer_();
  const configuredAdmins = _property_(NPS_LENS.adminEmailsProperty);
  if (configuredAdmins) {
    _assertAdmin_(viewer);
  } else if (!viewer.domainAllowed || viewer.adminSource !== 'initial-admin') {
    throw new Error('La configuración inicial debe realizarla el administrador inicial.');
  }
  const admins = String(adminEmails || '').split(',').map(value => value.trim().toLowerCase()).filter(Boolean);
  if (!admins.length || admins.some(email => !email.endsWith('@' + NPS_LENS.domain))) {
    throw new Error('Configura al menos un administrador del dominio BBVA.');
  }
  const cleanSpreadsheetId = String(spreadsheetId || '').trim();
  if (!cleanSpreadsheetId) throw new Error('Indica la hoja de cálculo de administración.');
  const book = SpreadsheetApp.openById(cleanSpreadsheetId);
  const specifications = [[NPS_LENS.activitySheet, ACTIVITY_HEADERS], [NPS_LENS.recipientsSheet, NEWSLETTER_RECIPIENT_HEADERS]];
  specifications.forEach(specification => {
    let sheet = book.getSheetByName(specification[0]);
    if (!sheet) sheet = book.insertSheet(specification[0]);
    if (!sheet.getLastRow()) sheet.appendRow(specification[1]);
    sheet.setFrozenRows(1);
  });
  PropertiesService.getScriptProperties().setProperties({
    NPS_LENS_SPREADSHEET_ID: cleanSpreadsheetId,
    [NPS_LENS.adminEmailsProperty]: admins.join(',')
  });
  return {ok: true, sheets: specifications.map(specification => specification[0])};
}
