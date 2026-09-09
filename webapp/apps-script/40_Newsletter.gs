const NEWSLETTER_RECIPIENT_HEADERS = Object.freeze([
  'audience_key', 'buug', 'n1', 'email', 'active', 'created_at', 'created_by', 'updated_at', 'updated_by'
]);

function _newsletterRecipients_(audienceKey) {
  const sheet = _sheet_(NPS_LENS.recipientsSheet);
  if (sheet.getLastRow() <= 1) return [];
  return sheet.getRange(2, 1, sheet.getLastRow() - 1, NEWSLETTER_RECIPIENT_HEADERS.length).getValues()
    .map((row, index) => ({row:index+2,audienceKey:String(row[0]),buug:String(row[1]),n1:String(row[2]),email:String(row[3]||'').toLowerCase(),active:row[4]===true}))
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
    presentationUrl: _presentationEntryUrl_(publication.scopeKey),
    sender: _newsletterSenderIdentity_(false)
  };
}

function _newsletterSubject_(publication) {
  const monthNames = {'01':'Enero','02':'Febrero','03':'Marzo','04':'Abril','05':'Mayo','06':'Junio','07':'Julio','08':'Agosto','09':'Septiembre','10':'Octubre','11':'Noviembre','12':'Diciembre'};
  return 'NPS e incidencias relacionadas - ' + publication.buug + ' ' + publication.n1 +
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
    const created = sheet.getRange(existing.row, 6, 1, 2).getValues()[0];
    sheet.getRange(existing.row, 5, 1, 5).setValues([[active, created[0], created[1], now, viewer.email]]);
  } else {
    sheet.appendRow([publication.audienceKey,publication.buug,publication.n1,email,active,now,viewer.email,now,viewer.email]);
  }
  return {email, active};
}

function _newsletterInsight_(edition) {
  const insight = edition && edition.newsletter;
  if (!insight || !Array.isArray(insight.scorecard) || !Array.isArray(insight.insights)) {
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

function _newsletterHtml_(insight, reportUrl, scopeKey) {
  const webUrl = ScriptApp.getService().getUrl() + '?source=newsletter&scope=' + encodeURIComponent(scopeKey);
  const e = _newsletterEscape_;
  const scorecard = (insight.scorecard || []).map(item => '<td width="20%" valign="top" style="padding:12px 9px;border-top:3px solid #85C8FF;background:#F7F8F8"><small style="color:#52627A;text-transform:uppercase">' + e(item.label) + '</small><br><b style="font:700 23px Georgia,serif;color:#001391">' + e(item.value) + '</b><br><small style="color:#52627A">' + e(item.delta) + '</small></td>').join('');
  const insights = (insight.insights || []).map(item => '<tr><td style="padding:15px 0;border-bottom:1px solid #D3D8E0"><b style="font:700 18px Georgia,serif;color:#070E46">' + e(item.title) + '</b><br><span style="color:#004481;font-weight:bold">' + e(item.evidence) + '</span><br><span style="color:#30375F">' + e(item.meaning) + '</span></td></tr>').join('');
  const focus = insight.focus || {}, focusRows = (focus.rows || []).map(item => '<tr><td style="padding:10px;border-bottom:1px solid #D3D8E0"><b>' + e(item.label) + '</b></td><td>' + e(item.nps) + '</td><td>' + e(item.delta) + '</td><td>' + e(item.detractors) + '</td><td>' + e(item.score) + '</td><td>' + e(item.opinions) + '</td></tr>').join('');
  const connections = (insight.connections || []).map(item => '<tr><td style="padding:17px 0;border-bottom:1px solid #D3D8E0"><b style="font:700 19px Georgia,serif;color:#070E46">' + e(item.topic) + '</b><div style="margin:7px 0;color:#004481;font-weight:bold">Cliente → tópico → ' + e(item.semantic_links) + ' vínculos semánticos → incidencias relacionadas</div>' + (item.comments || []).slice(0,1).map(text => '<div style="padding:9px 12px;background:#EAF3FA">“' + e(text) + '”</div>').join('') + (item.incidents || []).slice(0,2).map(incident => '<div style="font-size:12px;color:#52627A;margin-top:5px"><b>' + e(incident.id) + '</b> · ' + e(incident.summary) + '</div>').join('') + '<small style="color:#52627A">' + e(item.caveat) + '</small></td></tr>').join('');
  const quotes = (insight.quotes || []).slice(0,2).map(text => '<td width="50%" valign="top" style="padding:14px;background:#EAF3FA;border-left:3px solid #2DCCCD;font:italic 16px Georgia,serif">“' + e(text) + '”</td>').join('');
  const signals = (insight.signals || []).map(item => '<tr><td style="padding:10px 0;border-bottom:1px solid #D3D8E0"><b style="color:#070E46">' + e(item.label) + '</b><br><span style="color:#52627A">' + e(item.reason) + '</span></td></tr>').join('');
  return '<div style="font-family:Arial,sans-serif;color:#121F3F;max-width:680px;margin:auto;background:#fff"><div style="background:#070E46;color:#fff;padding:34px 38px"><small style="letter-spacing:1px">' + e(insight.brand) + '</small><h1 style="font:700 42px Georgia,serif;margin:22px 0 5px">' + e(insight.product) + '</h1><div>' + e(insight.promise) + '</div></div>' +
    '<div style="padding:32px 38px"><small style="color:#004481;text-transform:uppercase">Lectura de 30 segundos · ' + e(insight.period) + '</small><h2 style="font:700 31px Georgia,serif;color:#070E46;margin:10px 0">' + e(insight.headline) + '</h2><p style="line-height:1.55">' + e(insight.lead) + '</p><table role="presentation" width="100%" cellspacing="6"><tr>' + scorecard + '</tr></table>' +
    '<h2 style="font:700 26px Georgia,serif;color:#070E46;margin-top:30px">Lo que debes saber</h2><table role="presentation" width="100%">' + insights + '</table>' +
    '<h2 style="font:700 26px Georgia,serif;color:#070E46;margin-top:30px">Principal foco · ' + e(focus.title) + '</h2><table role="presentation" width="100%" cellspacing="0"><tr style="background:#070E46;color:#fff"><th>Palanca</th><th>NPS actual</th><th>Δ base</th><th>Detractores</th><th>Score</th><th>Opiniones</th></tr>' + focusRows + '</table>' +
    '<h2 style="font:700 26px Georgia,serif;color:#070E46;margin-top:30px">De la voz del cliente a la operación</h2><p style="color:#52627A">Relaciones semánticas observadas; no atribuyen causalidad.</p><table role="presentation" width="100%">' + connections + '</table>' +
    '<h2 style="font:700 26px Georgia,serif;color:#070E46;margin-top:30px">La voz del cliente</h2><table role="presentation" width="100%" cellspacing="8"><tr>' + quotes + '</tr></table><h2 style="font:700 26px Georgia,serif;color:#070E46;margin-top:30px">Señales a vigilar</h2><table role="presentation" width="100%">' + signals + '</table>' +
    '<p style="margin-top:30px;padding-top:22px;border-top:1px solid #D3D8E0"><a href="' + webUrl + '" style="display:inline-block;background:#001391;color:#fff;padding:13px 18px;text-decoration:none;font-weight:bold">Explorar NPS Lens</a>' + (reportUrl ? ' <a href="' + reportUrl + '" style="display:inline-block;color:#001391;padding:13px 18px;font-weight:bold">Ver análisis completo</a>' : '') + '</p></div></div>';
}

function _newsletterPlain_(insight, reportUrl, scopeKey) {
  const lines = [insight.brand, insight.product, insight.promise, insight.period, '', insight.headline, insight.lead, '', 'INDICADORES'];
  (insight.scorecard || []).forEach(item => lines.push(item.label + ': ' + item.value + (item.delta ? ' (' + item.delta + ')' : '')));
  lines.push('', 'LO QUE DEBES SABER'); (insight.insights || []).forEach(item => lines.push('• ' + item.title + ' — ' + item.evidence + '. ' + item.meaning));
  lines.push('', 'DE LA VOZ DEL CLIENTE A LA OPERACIÓN'); (insight.connections || []).forEach(item => lines.push('• ' + item.topic + ': ' + item.semantic_links + ' vínculos semánticos. ' + item.caveat));
  lines.push('', 'SEÑALES A VIGILAR'); (insight.signals || []).forEach(item => lines.push('• ' + item.label + ': ' + item.reason));
  lines.push('', 'Explorar NPS Lens: ' + ScriptApp.getService().getUrl() + '?source=newsletter&scope=' + encodeURIComponent(scopeKey));
  if (reportUrl) lines.push('Ver análisis completo: ' + reportUrl);
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
  if (!_reportUrl_(publication.scopeKey)) throw new Error('Publica primero una edición con su presentación nativa.');
  const reportUrl = _presentationEntryUrl_(publication.scopeKey);
  const sender = _newsletterSenderIdentity_(true);
  if (!sender.ready) throw new Error('El remitente corporativo ' + sender.requested +
    ' no está aceptado para ' + (sender.executor || 'el propietario del despliegue') +
    ' (' + sender.verificationStatus + '). ' + sender.message);
  const insight = _publishedNewsletterInsight_(publication.scopeKey);
  const html = _newsletterHtml_(insight, reportUrl, publication.scopeKey);
  const plain = _newsletterPlain_(insight, reportUrl, publication.scopeKey);
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
