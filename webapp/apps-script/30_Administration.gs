function getAdministration() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const publication = _selectedPublication_();
  return {
    version: NPS_LENS.version,
    generatedAt: publication ? publication.generatedAt : '',
    selectedScopeKey: publication ? publication.scopeKey : '',
    reportUrl: _reportUrl_(publication && publication.scopeKey),
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
  ScriptApp.requireAllScopes(ScriptApp.AuthMode.FULL);
  const viewer = _viewer_();
  const configuredAdmins = _property_(NPS_LENS.adminEmailsProperty);
  if (configuredAdmins) {
    _assertAdmin_(viewer);
  } else if (!viewer.domainAllowed || viewer.adminSource !== 'initial-admin') {
    throw new Error('La configuración inicial debe realizarla el administrador inicial.');
  }
  const adminSource = String(adminEmails || '').trim() || configuredAdmins || viewer.email;
  const admins = Array.from(new Set(adminSource.split(/[;,\s]+/)
    .map(value => value.trim().toLowerCase()).filter(Boolean)));
  const corporateEmail = new RegExp('^[^\\s@]+@' + NPS_LENS.domain.replace('.', '\\.') + '$');
  if (!admins.length || admins.some(email => !corporateEmail.test(email))) {
    throw new Error('Configura al menos un administrador del dominio BBVA.');
  }
  const configuredSpreadsheetId = _property_('NPS_LENS_SPREADSHEET_ID');
  const requestedSpreadsheetId = String(spreadsheetId || '').trim() || configuredSpreadsheetId;
  const created = !requestedSpreadsheetId;
  const book = created
    ? SpreadsheetApp.create('NPS Lens · Administración')
    : SpreadsheetApp.openById(requestedSpreadsheetId);
  const cleanSpreadsheetId = book.getId();
  const specifications = [[NPS_LENS.activitySheet, ACTIVITY_HEADERS], [NPS_LENS.recipientsSheet, NEWSLETTER_RECIPIENT_HEADERS], [NPS_LENS.publicationsSheet, PUBLICATION_HEADERS]];
  specifications.forEach(specification => {
    let sheet = book.getSheetByName(specification[0]);
    if (!sheet) sheet = book.insertSheet(specification[0]);
    if (!sheet.getLastRow()) sheet.appendRow(specification[1]);
    else {
      const current = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0].map(String);
      const expected = specification[1];
      if (current.join('\u0000') !== expected.join('\u0000')) {
        if (specification[0] === NPS_LENS.recipientsSheet && current.join(',') === 'email,active,created_at,created_by,updated_at,updated_by') {
          const legacy = sheet.getLastRow() > 1 ? sheet.getRange(2,1,sheet.getLastRow()-1,6).getValues() : [];
          sheet.clearContents(); sheet.getRange(1,1,1,expected.length).setValues([expected]);
          if (legacy.length) sheet.getRange(2,1,legacy.length,expected.length).setValues(legacy.map(row => ['', '', '', row[0], row[1], row[2], row[3], row[4], row[5]]));
        } else if (sheet.getLastRow() === 1) {
          sheet.clearContents(); sheet.getRange(1,1,1,expected.length).setValues([expected]);
        } else throw new Error('La hoja ' + specification[0] + ' tiene un contrato obsoleto con datos. Revísala antes de continuar.');
      }
    }
    sheet.setFrozenRows(1);
  });
  if (created) {
    const administrativeSheets = new Set(specifications.map(specification => specification[0]));
    book.getSheets().filter(sheet => !administrativeSheets.has(sheet.getName())).forEach(sheet => book.deleteSheet(sheet));
  }
  PropertiesService.getScriptProperties().setProperties({
    NPS_LENS_SPREADSHEET_ID: cleanSpreadsheetId,
    [NPS_LENS.adminEmailsProperty]: admins.join(',')
  });
  const result = {
    ok: true,
    version: NPS_LENS.version,
    created,
    spreadsheetId: cleanSpreadsheetId,
    spreadsheetUrl: book.getUrl(),
    administrators: admins,
    sheets: specifications.map(specification => specification[0]),
    webAppUrl: ScriptApp.getService().getUrl()
  };
  console.log(JSON.stringify(result));
  return result;
}
