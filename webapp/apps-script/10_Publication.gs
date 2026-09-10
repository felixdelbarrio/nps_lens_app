const PUBLICATION_HEADERS = Object.freeze([
  'scope_key', 'audience_key', 'buug', 'n1', 'n2', 'year', 'month', 'causal_method',
  'causal_method_label', 'edition_file_id', 'pptx_file_id', 'slides_file_id',
  'newsletter_insight', 'generated_at', 'imported_at', 'imported_by'
]);
const DRIVE_FOLDER_MIME = 'application/vnd.google-apps.folder';
const SNAPSHOT_CACHE_SECONDS = 21600;
const SNAPSHOT_CACHE_CHUNK_CHARS = 80000;
let publicationRowsCache = null;

function _publicationShellProperty_(scopeKey) {
  return 'NPS_LENS_SHELL_' + String(scopeKey || '').trim();
}

function _compactPptxProperty_(scopeKey) {
  return 'NPS_LENS_COMPACT_PPTX_' + String(scopeKey || '').trim();
}

function _compactSlidesProperty_(scopeKey) {
  return 'NPS_LENS_COMPACT_SLIDES_' + String(scopeKey || '').trim();
}

function _clearPublicationCache_() {
  publicationRowsCache = null;
  CacheService.getScriptCache().remove(_cacheKey_('publications'));
}

function _publicationRows_() {
  if (publicationRowsCache) return publicationRowsCache;
  const cacheKey = _cacheKey_('publications');
  const cached = CacheService.getScriptCache().get(cacheKey);
  if (cached) {
    publicationRowsCache = JSON.parse(cached);
    return publicationRowsCache;
  }
  const sheet = _sheet_(NPS_LENS.publicationsSheet);
  if (sheet.getLastRow() <= 1) return [];
  publicationRowsCache = sheet.getRange(2, 1, sheet.getLastRow() - 1, PUBLICATION_HEADERS.length).getValues()
    .map((row, index) => ({row: index + 2, scopeKey: String(row[0]), audienceKey: String(row[1]),
      buug: String(row[2]), n1: String(row[3]), n2: String(row[4]), year: String(row[5]),
      month: String(row[6]), causalMethod: String(row[7]), causalMethodLabel: String(row[8]),
      snapshotFileId: String(row[9]), pptxFileId: String(row[10]), slidesFileId: String(row[11]),
      newsletterInsight:String(row[12]||''),generatedAt:String(row[13]),importedAt:row[14],importedBy:String(row[15])}));
  const serialized = JSON.stringify(publicationRowsCache);
  if (serialized.length < 90000) CacheService.getScriptCache().put(cacheKey, serialized, 300);
  return publicationRowsCache;
}

function _publicationByKey_(scopeKey) {
  const key = String(scopeKey || '').trim();
  const rows = _publicationRows_();
  if (!rows.length) return null;
  return rows.find(item => item.scopeKey === key) || rows.sort((a, b) =>
    new Date(b.importedAt || 0).getTime() - new Date(a.importedAt || 0).getTime())[0];
}

function _selectedPublication_() { return _publicationByKey_(_property_(NPS_LENS.selectedScopeProperty)); }

function _validateEdition_(payload) {
  if (!payload || payload.schema_version !== '4.0' || !payload.manifest || !payload.screens || !payload.scope) {
    throw new Error('La edición no cumple el contrato NPS Lens 4.0 con ámbito inmutable.');
  }
  ['dashboard', 'linking', 'data'].forEach(name => {
    if (!payload.screens[name] || typeof payload.screens[name] !== 'object') throw new Error('Falta la pantalla requerida: ' + name + '.');
  });
  ['key','audience_key','buug','n1','year','month','causal_method','causal_method_label'].forEach(name => {
    if (!String(payload.scope[name] || '').trim()) throw new Error('Falta el dato de ámbito: ' + name + '.');
  });
  return payload;
}

function _validateArchive_(payload) {
  _validateEdition_(payload);
  const snapshot = payload.snapshots && payload.snapshots.data;
  const datasets = snapshot && snapshot.datasets;
  if (!snapshot || snapshot.schema_version !== '4.0' || !datasets || !datasets.nps || !datasets.helix) {
    throw new Error('La edición no incluye los snapshots estáticos calculados por la aplicación local.');
  }
  if (!datasets.nps.page || !datasets.helix.page) {
    throw new Error('Los snapshots estáticos están incompletos.');
  }
  return payload;
}

function _snapshotCacheKey_(fileId) {
  return _cacheKey_('snapshot-' + fileId);
}

function _cacheEncodedSnapshot_(fileId, encoded) {
  try {
    const chunks = [];
    for (let offset = 0; offset < encoded.length; offset += SNAPSHOT_CACHE_CHUNK_CHARS) {
      chunks.push(encoded.slice(offset, offset + SNAPSHOT_CACHE_CHUNK_CHARS));
    }
    const key = _snapshotCacheKey_(fileId), values = {};
    chunks.forEach((chunk, index) => { values[key + '-' + index] = chunk; });
    values[key] = String(chunks.length);
    CacheService.getScriptCache().putAll(values, SNAPSHOT_CACHE_SECONDS);
  } catch (error) {
    console.warn('No se pudo precalentar el snapshot ' + fileId + ': ' + error.message);
  }
}

function _encodedSnapshot_(fileId) {
  const cache = CacheService.getScriptCache(), key = _snapshotCacheKey_(fileId);
  const count = Number(cache.get(key));
  try {
    if (count > 0) {
      const keys = Array.from({length:count}, (_, index) => key + '-' + index);
      const values = cache.getAll(keys);
      if (keys.every(chunkKey => values[chunkKey])) {
        return keys.map(chunkKey => values[chunkKey]).join('');
      }
    }
  } catch (error) { cache.remove(key); }
  const blob = DriveApp.getFileById(fileId).getBlob(), bytes = blob.getBytes();
  const gzipBytes = bytes[0] === 31 && (bytes[1] === 139 || bytes[1] === -117)
    ? bytes : Utilities.gzip(Utilities.newBlob(bytes, 'application/json', 'snapshot.json')).getBytes();
  const encoded = Utilities.base64EncodeWebSafe(gzipBytes);
  _cacheEncodedSnapshot_(fileId, encoded);
  return encoded;
}

function _loadSnapshot_(fileId) {
  const bytes = Utilities.base64DecodeWebSafe(_encodedSnapshot_(fileId));
  const gzipBlob = Utilities.newBlob(bytes, 'application/gzip', 'snapshot.json.gz');
  return JSON.parse(Utilities.ungzip(gzipBlob).getDataAsString('UTF-8'));
}

function _publishedShell_(scopeKey) {
  const publication = _publicationByKey_(scopeKey);
  if (!publication) return {schema_version:'4.0',generated_at:'',screens:{},scope:{},manifest:{status:'Sin edición publicada'}};
  const fileId = _property_(_publicationShellProperty_(publication.scopeKey));
  if (!fileId) throw new Error('Esta edición debe volver a publicarse para aplicar la carga optimizada.');
  return _validateEdition_(_loadSnapshot_(fileId));
}

function _folderId_(reference) {
  const raw = String(reference || '').trim().replace(/^folder:/,'').replace(/^drive:/,'');
  const match = raw.match(/\/folders\/([A-Za-z0-9_-]+)/) || raw.match(/[?&]id=([A-Za-z0-9_-]+)/);
  const id = match ? match[1] : raw;
  if (!/^[A-Za-z0-9_-]{10,}$/.test(id)) throw new Error('Indica el enlace o identificador de una carpeta válida.');
  return id;
}

function _folderDescriptor_(reference) {
  const id = _folderId_(reference), cache = CacheService.getScriptCache();
  const cacheKey = _cacheKey_('folder-' + id), cached = cache.get(cacheKey);
  if (cached) return JSON.parse(cached);
  const resource = Drive.Files.get(id, {supportsAllDrives:true,fields:'id,name,mimeType,webViewLink,capabilities(canAddChildren)'});
  if (resource.mimeType !== DRIVE_FOLDER_MIME) throw new Error('La referencia no corresponde a una carpeta.');
  if (resource.capabilities && resource.capabilities.canAddChildren === false) throw new Error('No tienes permiso para publicar en esa carpeta.');
  const folder = {id:String(resource.id),name:String(resource.name || 'Carpeta de presentaciones'),url:String(resource.webViewLink || 'https://drive.google.com/drive/folders/' + encodeURIComponent(resource.id))};
  cache.put(cacheKey, JSON.stringify(folder), SNAPSHOT_CACHE_SECONDS);
  return folder;
}

function _configuredPublicationFolder_() {
  const id = _property_(NPS_LENS.publicationFolderProperty);
  return id ? _folderDescriptor_(id) : {id:'',name:'',url:''};
}

function getPublicationSettings() {
  const viewer = _viewer_(); _assertAdmin_(viewer);
  return _configuredPublicationFolder_();
}

function savePublicationFolder(reference) {
  const viewer = _viewer_(); _assertAdmin_(viewer);
  const folder = _folderDescriptor_(reference);
  PropertiesService.getScriptProperties().setProperty(NPS_LENS.publicationFolderProperty, folder.id);
  return folder;
}

function _publicationFolder_() {
  const id = _property_(NPS_LENS.publicationFolderProperty);
  if (!id) throw new Error('Configura primero la carpeta en Configuración > Publicación.');
  return _folderDescriptor_(id);
}

function _publicationCatalog_() {
  const showEvolutionNps = _evolutionNpsVisible_();
  return _publicationRows_().map(item => ({
    scopeKey:item.scopeKey,audienceKey:item.audienceKey,label:[item.buug,item.n1,item.year,item.month,item.causalMethodLabel].join(' · '),
    buug:item.buug,n1:item.n1,n2:item.n2,year:item.year,month:item.month,causalMethod:item.causalMethod,
    causalMethodLabel:item.causalMethodLabel,generatedAt:item.generatedAt,presentationUrl:_reportUrl_(item.scopeKey,showEvolutionNps)
  }));
}

function selectPublicationScope(scopeKey) {
  const viewer = _viewer_(); _assertAdmin_(viewer);
  const publication = _publicationRows_().find(item => item.scopeKey === String(scopeKey || ''));
  if (!publication) throw new Error('El ámbito seleccionado no está publicado.');
  PropertiesService.getScriptProperties().setProperty(NPS_LENS.selectedScopeProperty, publication.scopeKey);
  return getAdministration();
}

function importPublicationArchive(form) {
  const viewer = _viewer_(); _assertAdmin_(viewer);
  const archiveBlob = form && form.publication;
  if (!archiveBlob || typeof archiveBlob.getBytes !== 'function') throw new Error('Selecciona la edición ZIP generada por la aplicación local.');
  const archiveName = String(archiveBlob.getName() || 'publicacion.zip'), archiveBytes = archiveBlob.getBytes();
  if (!archiveName.toLowerCase().endsWith('.zip')) throw new Error('La edición debe ser un fichero ZIP.');
  if (!archiveBytes.length || archiveBytes.length > NPS_LENS.maxPublicationBytes) throw new Error('La edición debe ocupar entre 1 byte y 30 MB.');
  const members = Utilities.unzip(Utilities.newBlob(archiveBytes,'application/zip',archiveName));
  const editionBlob = members.find(blob => blob.getName() === 'publication.json');
  const reports = members.filter(blob => blob.getName().toLowerCase().endsWith('.pptx'));
  if (!editionBlob) throw new Error('La edición debe contener publication.json.');
  const edition = _validateArchive_(JSON.parse(editionBlob.getDataAsString('UTF-8')));
  const reportName = String(edition.manifest.report || '');
  const compactReportName = String(edition.manifest.report_without_evolution || '');
  const reportBlob = reports.find(blob => blob.getName() === reportName);
  const compactReportBlob = reports.find(blob => blob.getName() === compactReportName);
  if (!reportName || !compactReportName || reportName === compactReportName || !reportBlob || !compactReportBlob || reports.length !== 2) {
    throw new Error('Las presentaciones PPTX no coinciden con el manifiesto de la edición.');
  }
  const destination = _publicationFolder_(), previous = _publicationRows_().find(item => item.scopeKey === edition.scope.key);
  const shellProperty = _publicationShellProperty_(edition.scope.key), previousShellFileId = _property_(shellProperty);
  const compactPptxProperty = _compactPptxProperty_(edition.scope.key), previousCompactPptxFileId = _property_(compactPptxProperty);
  const compactSlidesProperty = _compactSlidesProperty_(edition.scope.key), previousCompactSlidesFileId = _property_(compactSlidesProperty);
  const previousSelectedScope = _property_(NPS_LENS.selectedScopeProperty);
  const newsletterInsight = JSON.stringify(_newsletterInsight_(edition));
  let snapshotFileId='', shellFileId='', pptxFileId='', slidesFileId='', compactPptxFileId='', compactSlidesFileId='', committed=false;
  try {
    const snapshotText = JSON.stringify(edition.snapshots.data);
    const snapshotBytes = Utilities.gzip(Utilities.newBlob(snapshotText,'application/json')).getBytes();
    const snapshotBlob = Utilities.newBlob(snapshotBytes,'application/gzip','nps-lens-'+edition.scope.key+'-data.json.gz');
    snapshotFileId = String(Drive.Files.create({name:snapshotBlob.getName(),mimeType:'application/gzip',parents:[destination.id]},snapshotBlob,{supportsAllDrives:true,fields:'id'}).id);
    delete edition.snapshots;
    const shellText = JSON.stringify(edition);
    const shellBytes = Utilities.gzip(Utilities.newBlob(shellText,'application/json')).getBytes();
    const shellBlob = Utilities.newBlob(shellBytes,'application/gzip','nps-lens-'+edition.scope.key+'-web.json.gz');
    shellFileId = String(Drive.Files.create({name:shellBlob.getName(),mimeType:'application/gzip',parents:[destination.id]},shellBlob,{supportsAllDrives:true,fields:'id'}).id);
    pptxFileId = String(Drive.Files.create({name:reportBlob.getName(),mimeType:'application/vnd.openxmlformats-officedocument.presentationml.presentation',parents:[destination.id]},reportBlob,{supportsAllDrives:true,fields:'id'}).id);
    slidesFileId = String(Drive.Files.create({name:reportBlob.getName().replace(/\.pptx$/i,''),mimeType:'application/vnd.google-apps.presentation',parents:[destination.id]},reportBlob,{supportsAllDrives:true,fields:'id'}).id);
    if (!slidesFileId) throw new Error('Google Drive no ha confirmado la presentación nativa.');
    compactPptxFileId = String(Drive.Files.create({name:compactReportBlob.getName(),mimeType:'application/vnd.openxmlformats-officedocument.presentationml.presentation',parents:[destination.id]},compactReportBlob,{supportsAllDrives:true,fields:'id'}).id);
    compactSlidesFileId = String(Drive.Files.create({name:compactReportBlob.getName().replace(/\.pptx$/i,''),mimeType:'application/vnd.google-apps.presentation',parents:[destination.id]},compactReportBlob,{supportsAllDrives:true,fields:'id'}).id);
    if (!compactSlidesFileId) throw new Error('Google Drive no ha confirmado la presentación compacta.');
    const s=edition.scope,row=[s.key,s.audience_key,s.buug,s.n1,s.n2||'',s.year,s.month,s.causal_method,s.causal_method_label,snapshotFileId,pptxFileId,slidesFileId,newsletterInsight,edition.generated_at,new Date(),viewer.email];
    const sheet=_sheet_(NPS_LENS.publicationsSheet);
    const properties = PropertiesService.getScriptProperties();
    properties.setProperties({[shellProperty]:shellFileId,[NPS_LENS.selectedScopeProperty]:s.key});
    properties.setProperty(compactPptxProperty,compactPptxFileId);
    properties.setProperty(compactSlidesProperty,compactSlidesFileId);
    if(previous) sheet.getRange(previous.row,1,1,PUBLICATION_HEADERS.length).setValues([row]); else sheet.appendRow(row);
    committed = true; _clearPublicationCache_();
    _cacheEncodedSnapshot_(shellFileId, Utilities.base64EncodeWebSafe(shellBytes));
    if(previous) [previous.snapshotFileId,previous.pptxFileId,previous.slidesFileId,previousShellFileId,previousCompactPptxFileId,previousCompactSlidesFileId].forEach(id=>{try{if(id)DriveApp.getFileById(id).setTrashed(true);}catch(error){}});
  } catch(error) {
    if (!committed) {
      const properties = PropertiesService.getScriptProperties();
      if(previousShellFileId) properties.setProperty(shellProperty,previousShellFileId); else properties.deleteProperty(shellProperty);
      if(previousCompactPptxFileId) properties.setProperty(compactPptxProperty,previousCompactPptxFileId); else properties.deleteProperty(compactPptxProperty);
      if(previousCompactSlidesFileId) properties.setProperty(compactSlidesProperty,previousCompactSlidesFileId); else properties.deleteProperty(compactSlidesProperty);
      if(previousSelectedScope) properties.setProperty(NPS_LENS.selectedScopeProperty,previousSelectedScope); else properties.deleteProperty(NPS_LENS.selectedScopeProperty);
      [snapshotFileId,shellFileId,pptxFileId,slidesFileId,compactPptxFileId,compactSlidesFileId].forEach(id=>{try{if(id)DriveApp.getFileById(id).setTrashed(true);}catch(ignore){}});
    }
    throw error;
  }
  return _administration_({scopeKey:edition.scope.key,generatedAt:edition.generated_at,slidesFileId}, viewer);
}

function _reportUrl_(scopeKey, showEvolutionNps) {
  const publication = _publicationByKey_(scopeKey);
  if (!publication) return '';
  const compactId = _property_(_compactSlidesProperty_(publication.scopeKey));
  const slidesId = showEvolutionNps ? publication.slidesFileId : compactId;
  return slidesId ? 'https://docs.google.com/presentation/d/' + encodeURIComponent(slidesId) + '/edit' : '';
}
