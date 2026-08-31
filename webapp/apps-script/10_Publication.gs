const PUBLICATION_HEADERS = Object.freeze([
  'scope_key', 'audience_key', 'buug', 'n1', 'n2', 'year', 'month', 'causal_method',
  'causal_method_label', 'edition_file_id', 'pptx_file_id', 'slides_file_id',
  'newsletter_insight', 'generated_at', 'imported_at', 'imported_by'
]);
const DRIVE_FOLDER_MIME = 'application/vnd.google-apps.folder';
const PUBLICATION_CACHE_KEY = 'nps-lens-publications-' + NPS_LENS.version;
const SNAPSHOT_CACHE_SECONDS = 21600;
const SNAPSHOT_CACHE_CHUNK_CHARS = 80000;
let publicationRowsCache = null;

function _publicationShellProperty_(scopeKey) {
  return 'NPS_LENS_SHELL_' + String(scopeKey || '').trim();
}

function _clearPublicationCache_() {
  publicationRowsCache = null;
  CacheService.getScriptCache().remove(PUBLICATION_CACHE_KEY);
}

function _publicationRows_() {
  if (publicationRowsCache) return publicationRowsCache;
  const cached = CacheService.getScriptCache().get(PUBLICATION_CACHE_KEY);
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
  if (serialized.length < 90000) CacheService.getScriptCache().put(PUBLICATION_CACHE_KEY, serialized, 300);
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
  if (!payload || payload.schema_version !== '1.0' || !payload.manifest || !payload.screens || !payload.scope) {
    throw new Error('La edición no cumple el contrato NPS Lens 1.0 con ámbito inmutable.');
  }
  ['dashboard', 'linking', 'data'].forEach(name => {
    if (!payload.screens[name] || typeof payload.screens[name] !== 'object') throw new Error('Falta la pantalla requerida: ' + name + '.');
  });
  ['key','audience_key','buug','n1','year','month','causal_method','causal_method_label'].forEach(name => {
    if (!String(payload.scope[name] || '').trim()) throw new Error('Falta el dato de ámbito: ' + name + '.');
  });
  return payload;
}

function _snapshotCacheKey_(fileId) {
  return 'nps-lens-snapshot-' + NPS_LENS.version + '-' + fileId;
}

function _cacheSnapshot_(fileId, jsonText) {
  try {
    const encoded = Utilities.base64EncodeWebSafe(Utilities.gzip(
      Utilities.newBlob(jsonText, 'application/json')).getBytes());
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

function _loadSnapshot_(fileId) {
  const cache = CacheService.getScriptCache(), key = _snapshotCacheKey_(fileId);
  const count = Number(cache.get(key));
  try {
    if (count > 0) {
      const keys = Array.from({length:count}, (_, index) => key + '-' + index);
      const values = cache.getAll(keys);
      if (keys.every(chunkKey => values[chunkKey])) {
        const bytes = Utilities.base64DecodeWebSafe(keys.map(chunkKey => values[chunkKey]).join(''));
        return JSON.parse(Utilities.ungzip(Utilities.newBlob(bytes)).getDataAsString('UTF-8'));
      }
    }
  } catch (error) { cache.remove(key); }
  const text = DriveApp.getFileById(fileId).getBlob().getDataAsString('UTF-8');
  _cacheSnapshot_(fileId, text);
  return JSON.parse(text);
}

function _normalizedDatasetFilter_(value) {
  return String(value || 'todos').trim().toLowerCase()
    .replace(/^detractores$/, 'detractor').replace(/^promotores$/, 'promotor').replace(/^neutros$/, 'pasivo');
}

function _datasetSnapshot_(edition) {
  const sources = edition.screens && edition.screens.data || {}, datasets = {};
  ['nps','helix'].forEach(kind => {
    const source = sources[kind] || {}, rows = Array.isArray(source.rows) ? source.rows : [];
    const columns = Array.isArray(source.columns) ? source.columns.map(String) : [];
    const visibleColumns = columns.slice(0, 14);
    const page = selected => ({total_rows:selected.length,rows:selected.slice(0, 500).map(row => {
      const projected = {}; visibleColumns.forEach(column => { projected[column] = row[column]; }); return projected;
    })});
    if (kind === 'helix') {
      datasets.helix = {columns,total_rows:rows.length,page:page(rows)};
      return;
    }
    const channels = Array.from(new Set(['todos'].concat(rows.map(row => _normalizedDatasetFilter_(row.Canal)))));
    const groups = Array.from(new Set(['todos'].concat(rows.map(row => _normalizedDatasetFilter_(row['NPS Group'])))));
    const pages = {};
    channels.forEach(channel => groups.forEach(group => {
      const selected = rows.filter(row => (channel === 'todos' || _normalizedDatasetFilter_(row.Canal) === channel) &&
        (group === 'todos' || _normalizedDatasetFilter_(row['NPS Group']) === group));
      pages[channel + '|' + group] = page(selected);
    }));
    datasets.nps = {columns,total_rows:rows.length,pages};
  });
  return {schema_version:'1.0',datasets};
}

function _publishedShell_(scopeKey) {
  const publication = _publicationByKey_(scopeKey);
  if (!publication) return {schema_version:'1.0',generated_at:'',screens:{},scope:{},manifest:{status:'Sin edición publicada'}};
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
  const cacheKey = 'nps-lens-folder-' + NPS_LENS.version + '-' + id, cached = cache.get(cacheKey);
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

function savePublicationFolder(reference) {
  const viewer = _viewer_(); _assertAdmin_(viewer);
  const folder = _folderDescriptor_(reference);
  PropertiesService.getScriptProperties().setProperty(NPS_LENS.publicationFolderProperty, folder.id);
  return folder;
}

function _publicationFolder_() {
  const id = _property_(NPS_LENS.publicationFolderProperty);
  if (!id) throw new Error('Configura primero la carpeta de presentaciones en Newsletter.');
  return _folderDescriptor_(id);
}

function _publicationCatalog_() {
  return _publicationRows_().map(item => ({
    scopeKey:item.scopeKey,audienceKey:item.audienceKey,label:[item.buug,item.n1,item.year,item.month,item.causalMethodLabel].join(' · '),
    buug:item.buug,n1:item.n1,n2:item.n2,year:item.year,month:item.month,causalMethod:item.causalMethod,
    causalMethodLabel:item.causalMethodLabel,generatedAt:item.generatedAt,presentationUrl:'https://docs.google.com/presentation/d/'+encodeURIComponent(item.slidesFileId)+'/edit'
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
  if (!editionBlob || reports.length !== 1) throw new Error('La edición debe contener publication.json y una única presentación PPTX.');
  const edition = _validateEdition_(JSON.parse(editionBlob.getDataAsString('UTF-8'))), reportBlob = reports[0];
  if (String(edition.manifest.report || '') !== reportBlob.getName()) throw new Error('La presentación no coincide con el manifiesto.');
  const destination = _publicationFolder_(), previous = _publicationRows_().find(item => item.scopeKey === edition.scope.key);
  const shellProperty = _publicationShellProperty_(edition.scope.key), previousShellFileId = _property_(shellProperty);
  const previousSelectedScope = _property_(NPS_LENS.selectedScopeProperty);
  const newsletterInsight = JSON.stringify(_newsletterInsight_(edition));
  let snapshotFileId='', shellFileId='', pptxFileId='', slidesFileId='', committed=false;
  try {
    const snapshotText = JSON.stringify(_datasetSnapshot_(edition));
    const snapshotBlob = Utilities.newBlob(snapshotText,'application/json','nps-lens-'+edition.scope.key+'-data.json');
    snapshotFileId = String(Drive.Files.create({name:snapshotBlob.getName(),mimeType:'application/json',parents:[destination.id]},snapshotBlob,{supportsAllDrives:true,fields:'id'}).id);
    const data = edition.screens && edition.screens.data || {};
    Object.keys(data).forEach(kind => { if (data[kind] && Array.isArray(data[kind].rows)) {
      data[kind].rows = []; data[kind].deferred = true;
    }});
    const shellText = JSON.stringify(edition);
    const shellBlob = Utilities.newBlob(shellText,'application/json','nps-lens-'+edition.scope.key+'-web.json');
    shellFileId = String(Drive.Files.create({name:shellBlob.getName(),mimeType:'application/json',parents:[destination.id]},shellBlob,{supportsAllDrives:true,fields:'id'}).id);
    pptxFileId = String(Drive.Files.create({name:reportBlob.getName(),mimeType:'application/vnd.openxmlformats-officedocument.presentationml.presentation',parents:[destination.id]},reportBlob,{supportsAllDrives:true,fields:'id'}).id);
    slidesFileId = String(Drive.Files.create({name:reportBlob.getName().replace(/\.pptx$/i,''),mimeType:'application/vnd.google-apps.presentation',parents:[destination.id]},reportBlob,{supportsAllDrives:true,fields:'id'}).id);
    if (!slidesFileId || !SlidesApp.openById(slidesFileId).getSlides().length) throw new Error('La presentación nativa no contiene diapositivas.');
    const s=edition.scope,row=[s.key,s.audience_key,s.buug,s.n1,s.n2||'',s.year,s.month,s.causal_method,s.causal_method_label,snapshotFileId,pptxFileId,slidesFileId,newsletterInsight,edition.generated_at,new Date(),viewer.email];
    const sheet=_sheet_(NPS_LENS.publicationsSheet);
    PropertiesService.getScriptProperties().setProperties({[shellProperty]:shellFileId,[NPS_LENS.selectedScopeProperty]:s.key});
    if(previous) sheet.getRange(previous.row,1,1,PUBLICATION_HEADERS.length).setValues([row]); else sheet.appendRow(row);
    committed = true; _clearPublicationCache_();
    _cacheSnapshot_(snapshotFileId, snapshotText); _cacheSnapshot_(shellFileId, shellText);
    if(previous) [previous.snapshotFileId,previous.pptxFileId,previous.slidesFileId,previousShellFileId].forEach(id=>{try{if(id)DriveApp.getFileById(id).setTrashed(true);}catch(error){}});
  } catch(error) {
    if (!committed) {
      const properties = PropertiesService.getScriptProperties();
      if(previousShellFileId) properties.setProperty(shellProperty,previousShellFileId); else properties.deleteProperty(shellProperty);
      if(previousSelectedScope) properties.setProperty(NPS_LENS.selectedScopeProperty,previousSelectedScope); else properties.deleteProperty(NPS_LENS.selectedScopeProperty);
      [snapshotFileId,shellFileId,pptxFileId,slidesFileId].forEach(id=>{try{if(id)DriveApp.getFileById(id).setTrashed(true);}catch(ignore){}});
    }
    throw error;
  }
  return getAdministration();
}

function _reportUrl_(scopeKey) {
  const publication = _publicationByKey_(scopeKey);
  return publication ? 'https://docs.google.com/presentation/d/' + encodeURIComponent(publication.slidesFileId) + '/edit' : '';
}
