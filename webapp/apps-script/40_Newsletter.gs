const NEWSLETTER_RECIPIENT_HEADERS = Object.freeze([
  'audience_key', 'owner_support_company', 'email', 'active', 'created_at', 'created_by', 'updated_at', 'updated_by'
]);

function _newsletterRecipients_(audienceKey) {
  const sheet = _sheet_(NPS_LENS.recipientsSheet);
  if (sheet.getLastRow() <= 1) return [];
  return sheet.getRange(2, 1, sheet.getLastRow() - 1, NEWSLETTER_RECIPIENT_HEADERS.length).getValues()
    .map((row, index) => ({row:index+2,audienceKey:String(row[0]),ownerSupportCompany:String(row[1]),email:String(row[2]||'').toLowerCase(),active:row[3]===true}))
    .filter(item => !audienceKey || item.audienceKey === audienceKey);
}

function _newsletterSenderIdentity_(forceRefresh) {
  const requested = NPS_LENS.newsletterFrom.toLowerCase();
  const executor = String(Session.getEffectiveUser().getEmail() || '').trim().toLowerCase();
  const cache = CacheService.getScriptCache();
  const cacheKey = _cacheKey_('newsletter-sender');
  if (forceRefresh === true) cache.remove(cacheKey);
  else {
    const stored = cache.get(cacheKey);
    if (stored) {
      try { return JSON.parse(stored); } catch (error) { cache.remove(cacheKey); }
    }
  }
  let response;
  try {
    response = Gmail.Users.Settings.SendAs.list('me');
  } catch (error) {
    const unavailable = {requested, effective: '', name: NPS_LENS.newsletterSenderName,
      executor, ready: false, verificationStatus: 'unavailable',
      message: 'Autoriza el servicio de Gmail y vuelve a validar el buzón corporativo.'};
    cache.put(cacheKey, JSON.stringify(unavailable), 15);
    return unavailable;
  }
  const sendAs = (response.sendAs || []).find(item =>
    String(item.sendAsEmail || '').trim().toLowerCase() === requested);
  const verificationStatus = String(sendAs && sendAs.verificationStatus || 'not-configured').toLowerCase();
  const ready = Boolean(sendAs) && verificationStatus === 'accepted';
  const identity = {requested, effective: ready ? requested : '', name: NPS_LENS.newsletterSenderName,
    executor, ready, verificationStatus,
    message: ready ? 'Buzón corporativo preparado.' :
      'Añade y verifica este buzón en Gmail > Configuración > Cuentas > Enviar correo como.'};
  cache.put(cacheKey, JSON.stringify(identity), ready ? 300 : 15);
  return identity;
}

function revalidateNewsletterSender() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  return _newsletterSenderIdentity_(true);
}

function getNewsletterWorkspace() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const publication = _selectedPublication_();
  if (!publication) return {subject:'Importa un ámbito para construir el asunto',scope:null,
    recipients:[],activeCount:0,presentationUrl:'',sender:_newsletterSenderIdentity_(false)};
  const recipients = _newsletterRecipients_(publication.audienceKey);
  return {
    subject: _newsletterSubject_(publication),
    scope: publication,
    recipients: recipients.map(item => ({email: item.email, active: item.active})),
    activeCount: recipients.filter(item => item.active).length,
    presentationUrl: _reportUrl_(publication.scopeKey,_evolutionNpsVisible_()),
    sender: _newsletterSenderIdentity_(false)
  };
}

function _newsletterSubject_(publication) {
  const monthNames = {'01':'Enero','02':'Febrero','03':'Marzo','04':'Abril','05':'Mayo','06':'Junio','07':'Julio','08':'Agosto','09':'Septiembre','10':'Octubre','11':'Noviembre','12':'Diciembre'};
  return 'NPS e incidencias relacionadas - ' + publication.ownerSupportCompany +
    ' - ' + publication.year + ' ' + (monthNames[publication.month] || publication.month) + ' - ' + publication.causalMethodLabel;
}

function saveNewsletterRecipient(payload) {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const email = _cleanText_(payload && payload.email, 180).toLowerCase();
  if (!/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) || !email.endsWith('@' + NPS_LENS.domain)) {
    throw new Error('Indica un correo válido del dominio BBVA.');
  }
  const active = !payload || payload.active !== false;
  const publication = _selectedPublication_();
  if (!publication) throw new Error('Selecciona primero el ámbito de la newsletter.');
  const sheet = _sheet_(NPS_LENS.recipientsSheet);
  const existing = _newsletterRecipients_(publication.audienceKey).find(item => item.email === email);
  const now = new Date();
  if (existing) {
    const created = sheet.getRange(existing.row, 5, 1, 2).getValues()[0];
    sheet.getRange(existing.row, 4, 1, 5).setValues([[active, created[0], created[1], now, viewer.email]]);
  } else {
    sheet.appendRow([publication.audienceKey,publication.ownerSupportCompany,email,active,now,viewer.email,now,viewer.email]);
  }
  return {email, active};
}

function _newsletterInsight_(edition) {
  const insight = edition && edition.newsletter;
  if (!insight || !Array.isArray(insight.scorecard) || !Array.isArray(insight.quotes) || !Array.isArray(insight.signals)) {
    throw new Error('La edición no contiene el modelo editorial de newsletter. Genérala de nuevo desde NPS Lens.');
  }
  return insight;
}

function _newsletterEscape_(value) {
  return String(value == null ? '' : value).replace(/[&<>"']/g, character => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  })[character]);
}

function _publishedNewsletterInsight_(scopeKey) {
  const publication = _publicationByKey_(scopeKey);
  if (!publication || !publication.newsletterInsight) throw new Error('El ámbito no contiene el insight de newsletter. Vuelve a importarlo.');
  try { return JSON.parse(publication.newsletterInsight); }
  catch (error) { throw new Error('El insight de newsletter no es válido. Vuelve a importar el ámbito.'); }
}

function _newsletterHtml_(insight, reportUrl, scopeKey, showEvolutionNps) {
  const webUrl = ScriptApp.getService().getUrl() + '?source=newsletter&scope=' + encodeURIComponent(scopeKey);
  const e = _newsletterEscape_;
  const scorecard = (insight.scorecard || []).map(item => '<td class="metric-cell" width="20%" valign="top" style="padding:12px 9px;border-top:3px solid #85C8FF;background:#F7F8F8;overflow-wrap:anywhere"><small style="color:#52627A;text-transform:uppercase">' + e(item.label) + '</small><br><b style="font:700 23px Georgia,serif;color:#001391">' + e(item.value) + '</b><br><small style="color:#52627A">' + e(item.delta) + '</small></td>').join('');
  const quotes = (insight.quotes || []).slice(0,2).map(text => '<td class="quote-cell" width="50%" valign="top" style="padding:14px;background:#EAF3FA;border-left:3px solid #2DCCCD;font:italic 16px Georgia,serif;overflow-wrap:anywhere">“' + e(text) + '”</td>').join('');
  const signals = (insight.signals || []).map(item => '<tr><td style="padding:10px 0;border-bottom:1px solid #D3D8E0"><b style="color:#070E46">' + e(item.label) + '</b><br><span style="color:#52627A">' + e(item.reason) + '</span></td></tr>').join('');
  const headline = showEvolutionNps ? '<h2 class="email-headline" style="font:700 31px Georgia,serif;color:#070E46;margin:10px 0">' + e(insight.headline) + '</h2><p style="line-height:1.55">' + e(insight.lead) + '</p>' : '';
  const reportButton = reportUrl ? '<a href="' + reportUrl + '" style="display:inline-block;background:#001391;color:#fff;padding:13px 18px;text-decoration:none;font-weight:bold">Ver análisis completo</a> ' : '';
  const mobileCss = '<style>body{margin:0!important;padding:0!important;width:100%!important;background:#F4F6F8}td,th,p,h1,h2,div,a{overflow-wrap:anywhere;word-break:normal}img{max-width:100%;height:auto}@media only screen and (max-width:600px){.email-shell-padding{padding:0!important}.email-card{width:100%!important;max-width:100%!important}.email-hero{padding:24px 20px!important}.email-body{padding:24px 18px!important}.email-title{font-size:34px!important;line-height:1.05!important}.email-headline{font-size:27px!important;line-height:1.12!important}.metric-table,.metric-table tbody,.metric-table tr,.quote-table,.quote-table tbody,.quote-table tr{display:block!important;width:100%!important;box-sizing:border-box!important}.metric-table,.quote-table{border-spacing:0 8px!important}.metric-cell,.quote-cell{display:block!important;width:auto!important;margin:0 0 8px!important}.email-actions a{display:block!important;margin:8px 0!important;text-align:center!important}.email-body>table{max-width:100%!important}}</style>';
  return '<!doctype html><html lang="es"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">' + mobileCss + '</head><body><table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="width:100%;background:#F4F6F8"><tr><td class="email-shell-padding" align="center" style="padding:24px 10px"><table class="email-card" role="presentation" width="680" cellspacing="0" cellpadding="0" style="width:100%;max-width:680px;background:#fff"><tr><td class="email-hero" style="background:#070E46;color:#fff;padding:34px 38px"><small style="letter-spacing:1px">' + e(insight.brand) + '</small><h1 class="email-title" style="font:700 42px Georgia,serif;margin:22px 0 5px">' + e(insight.product) + '</h1><div>' + e(insight.promise) + '</div></td></tr><tr><td class="email-body" style="padding:32px 38px">' +
    '<small style="color:#004481;text-transform:uppercase">Lectura de 30 segundos · ' + e(insight.period) + '</small>' + headline + '<table class="metric-table" role="presentation" width="100%" cellspacing="6"><tr>' + scorecard + '</tr></table>' +
    '<h2 style="font:700 26px Georgia,serif;color:#070E46;margin-top:30px">La voz del cliente</h2><table class="quote-table" role="presentation" width="100%" cellspacing="8"><tr>' + quotes + '</tr></table>' +
    '<h2 style="font:700 26px Georgia,serif;color:#070E46;margin-top:30px">Señales a vigilar</h2><table role="presentation" width="100%">' + signals + '</table>' +
    '<p class="email-actions" style="margin-top:30px;padding-top:22px;border-top:1px solid #D3D8E0">' + reportButton + '<a href="' + webUrl + '" style="display:inline-block;background:#004481;color:#fff;padding:13px 18px;text-decoration:none;font-weight:bold">Explorar NPS Lens</a></p></td></tr></table></td></tr></table></body></html>';
}

function _newsletterPlain_(insight, reportUrl, scopeKey, showEvolutionNps) {
  const lines = [insight.brand, insight.product, insight.promise, insight.period];
  if (showEvolutionNps) lines.push('', insight.headline, insight.lead);
  lines.push('', 'INDICADORES');
  (insight.scorecard || []).forEach(item => lines.push(item.label + ': ' + item.value + (item.delta ? ' (' + item.delta + ')' : '')));
  lines.push('', 'LA VOZ DEL CLIENTE'); (insight.quotes || []).slice(0,2).forEach(text => lines.push('• “' + text + '”'));
  lines.push('', 'SEÑALES A VIGILAR'); (insight.signals || []).forEach(item => lines.push('• ' + item.label + ': ' + item.reason));
  if (reportUrl) lines.push('Ver análisis completo: ' + reportUrl);
  lines.push('Explorar NPS Lens: ' + ScriptApp.getService().getUrl() + '?source=newsletter&scope=' + encodeURIComponent(scopeKey));
  return lines.filter(value => value !== null && value !== undefined).join('\n');
}

function _newsletterEncodedHeader_(value) {
  return '=?UTF-8?B?' + Utilities.base64Encode(String(value || ''), Utilities.Charset.UTF_8) + '?=';
}

function _newsletterMimeText_(value) {
  const encoded = Utilities.base64Encode(String(value || ''), Utilities.Charset.UTF_8);
  return (encoded.match(/.{1,76}/g) || ['']).join('\r\n');
}

function _newsletterMimeMessage_(recipient, subject, html, plain, sender) {
  const token = Utilities.getUuid().replace(/-/g, '');
  const boundary = 'nps_lens_' + token;
  const crlf = '\r\n';
  const mime = [
    'From: ' + _newsletterEncodedHeader_(NPS_LENS.newsletterSenderName) + ' <' + sender.effective + '>',
    'Reply-To: ' + sender.effective,
    'To: ' + recipient,
    'Subject: ' + _newsletterEncodedHeader_(subject),
    'Date: ' + new Date().toUTCString(),
    'Message-ID: <' + token + '@nps-lens.bbva.com>',
    'X-NPS-Lens-Version: ' + NPS_LENS.version,
    'MIME-Version: 1.0',
    'Content-Type: multipart/alternative; boundary="' + boundary + '"', '',
    '--' + boundary, 'Content-Type: text/plain; charset="UTF-8"',
    'Content-Transfer-Encoding: base64', '', _newsletterMimeText_(plain),
    '--' + boundary, 'Content-Type: text/html; charset="UTF-8"',
    'Content-Transfer-Encoding: base64', '', _newsletterMimeText_(html),
    '--' + boundary + '--', ''
  ].join(crlf);
  return Utilities.base64EncodeWebSafe(mime, Utilities.Charset.UTF_8).replace(/=+$/g, '');
}

function _sendNewsletterTo_(recipients, subject, publication) {
  const showEvolutionNps = _evolutionNpsVisible_();
  const reportUrl = _reportUrl_(publication.scopeKey, showEvolutionNps);
  if (!reportUrl) throw new Error('Publica primero una edición con su presentación nativa.');
  const sender = _newsletterSenderIdentity_(true);
  if (!sender.ready) throw new Error('El remitente corporativo ' + sender.requested +
    ' no está aceptado para ' + (sender.executor || 'el propietario del despliegue') +
    ' (' + sender.verificationStatus + '). ' + sender.message);
  const insight = _publishedNewsletterInsight_(publication.scopeKey);
  const html = _newsletterHtml_(insight, reportUrl, publication.scopeKey, showEvolutionNps);
  const plain = _newsletterPlain_(insight, reportUrl, publication.scopeKey, showEvolutionNps);
  const messageIds = recipients.map(recipient => {
    const raw = _newsletterMimeMessage_(recipient, subject, html, plain, sender);
    const accepted = Gmail.Users.Messages.send({raw}, 'me');
    if (!accepted || !accepted.id) throw new Error('Gmail no ha confirmado el envío a ' + recipient + '.');
    return String(accepted.id);
  });
  return {sent: recipients.length, at: new Date().toISOString(), presentationUrl: reportUrl,
    sender: sender.effective, senderName: NPS_LENS.newsletterSenderName, messageIds};
}

function testNewsletter() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const publication = _selectedPublication_();
  if (!publication) throw new Error('Selecciona primero un ámbito.');
  return _sendNewsletterTo_([viewer.email], '[PRUEBA] ' + _newsletterSubject_(publication), publication);
}

function sendNewsletter() {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const settings = getNewsletterWorkspace();
  const active = settings.recipients.filter(item => item.active).map(item => item.email);
  if (!active.length) throw new Error('Activa al menos un destinatario.');
  return _sendNewsletterTo_(active, settings.subject, _selectedPublication_());
}
