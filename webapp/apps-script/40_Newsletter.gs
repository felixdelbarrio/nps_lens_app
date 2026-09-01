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
    presentationUrl: _reportUrl_(publication.scopeKey),
    sender: _newsletterSenderIdentity_(false)
  };
}

function _newsletterSubject_(publication) {
  const monthNames = {'01':'Enero','02':'Febrero','03':'Marzo','04':'Abril','05':'Mayo','06':'Junio','07':'Julio','08':'Agosto','09':'Septiembre','10':'Octubre','11':'Noviembre','12':'Diciembre'};
  return 'NPS y causalidad con incidencias - ' + publication.buug + ' ' + publication.n1 +
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
  const dashboard = edition.screens && edition.screens.dashboard || {};
  const kpis = dashboard.kpis || {};
  const opportunities = dashboard.opportunities && dashboard.opportunities.table || [];
  const opportunity = opportunities.length ? String(opportunities[0].opportunity || opportunities[0].topic || opportunities[0].name || '') : '';
  return {context: _cleanText_(dashboard.context_label || 'Edición actualizada', 240),
    samples: kpis.samples == null ? 'n/d' : String(kpis.samples),
    nps: kpis.classic_nps == null ? 'n/d' : String(kpis.classic_nps), opportunity: _cleanText_(opportunity, 240)};
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
  const webUrl = ScriptApp.getService().getUrl() + '?scope=' + encodeURIComponent(scopeKey);
  const opportunity = insight.opportunity ? '<div style="margin:20px 0;padding:18px;background:#EAF3FA;border-left:4px solid #2DCCCD"><b>Foco ejecutivo</b><br>' + _newsletterEscape_(insight.opportunity) + '</div>' : '';
  return '<div style="font-family:Arial,sans-serif;color:#121F3F;max-width:680px;margin:auto;background:#F4F6F8">' +
    '<div style="background:#070E46;color:#fff;padding:32px"><div style="font-size:12px;letter-spacing:1.2px">BBVA BANCA DE EMPRESAS E INSTITUCIONES</div><h1 style="margin:12px 0 4px">NPS Lens</h1><div>La voz del cliente conectada con la operación</div></div>' +
    '<div style="padding:32px;background:#fff"><h2 style="font-family:Georgia,serif;color:#070E46">Una lectura preparada para decidir</h2><p>' + _newsletterEscape_(insight.context) + '</p>' +
    '<div style="display:flex;gap:12px"><div style="padding:14px;background:#F4F6F8;min-width:120px"><small>Muestras</small><br><b style="font-size:24px">' + _newsletterEscape_(insight.samples) + '</b></div><div style="padding:14px;background:#F4F6F8;min-width:120px"><small>NPS clásico</small><br><b style="font-size:24px">' + _newsletterEscape_(insight.nps) + '</b></div></div>' + opportunity +
    '<p><a href="' + webUrl + '" style="display:inline-block;background:#001391;color:#fff;padding:13px 18px;text-decoration:none;font-weight:bold">Abrir NPS Lens</a>' +
    (reportUrl ? ' <a href="' + reportUrl + '" style="display:inline-block;color:#001391;padding:13px 18px;font-weight:bold">Abrir presentación en Google Slides</a>' : '') + '</p></div></div>';
}

function _newsletterPlain_(insight, reportUrl, scopeKey) {
  return ['BBVA Banca de Empresas e Instituciones', 'NPS Lens', '', insight.context,
    'Muestras: ' + insight.samples, 'NPS clásico: ' + insight.nps,
    insight.opportunity ? 'Foco ejecutivo: ' + insight.opportunity : '', '',
    'Abrir NPS Lens: ' + ScriptApp.getService().getUrl() + '?scope=' + encodeURIComponent(scopeKey),
    reportUrl ? 'Abrir presentación: ' + reportUrl : ''].filter(Boolean).join('\n');
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
  const reportUrl = _reportUrl_(publication.scopeKey);
  if (!reportUrl) throw new Error('Publica primero una edición con su presentación nativa.');
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
