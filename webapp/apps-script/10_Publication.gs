function _publishedEdition_() {
  const fileId = _property_(NPS_LENS.editionFileProperty);
  if (!fileId) return {schema_version: '1.0', generated_at: '', screens: {}, manifest: {status: 'Sin edición publicada'}};
  const content = DriveApp.getFileById(fileId).getBlob().getDataAsString('UTF-8');
  return _validateEdition_(JSON.parse(content));
}

function _validateEdition_(payload) {
  if (!payload || payload.schema_version !== '1.0' || !payload.manifest || !payload.screens) {
    throw new Error('La edición no cumple el contrato NPS Lens 1.0.');
  }
  ['dashboard', 'linking', 'data'].forEach(name => {
    if (!payload.screens[name] || typeof payload.screens[name] !== 'object') {
      throw new Error('La edición no contiene la pantalla requerida: ' + name + '.');
    }
  });
  return payload;
}

function _publicationFolder_() {
  const properties = PropertiesService.getScriptProperties();
  const folderId = _property_(NPS_LENS.publicationFolderProperty);
  if (folderId) return DriveApp.getFolderById(folderId);
  const folder = DriveApp.createFolder('NPS Lens · Publicaciones Web');
  properties.setProperty(NPS_LENS.publicationFolderProperty, folder.getId());
  return folder;
}

function importPublicationArchive(form) {
  const viewer = _viewer_();
  _assertAdmin_(viewer);
  const archiveBlob = form && form.publication;
  if (!archiveBlob || typeof archiveBlob.getBytes !== 'function') {
    throw new Error('Selecciona la edición ZIP generada por la aplicación local.');
  }
  const archiveName = String(archiveBlob.getName() || 'publicacion.zip');
  const archiveBytes = archiveBlob.getBytes();
  if (!archiveName.toLowerCase().endsWith('.zip')) throw new Error('La edición debe ser un fichero ZIP.');
  if (!archiveBytes.length || archiveBytes.length > NPS_LENS.maxPublicationBytes) {
    throw new Error('La edición debe ocupar entre 1 byte y 30 MB.');
  }

  const members = Utilities.unzip(Utilities.newBlob(archiveBytes, 'application/zip', archiveName));
  const editionBlob = members.find(blob => blob.getName() === 'publication.json');
  const reportBlobs = members.filter(blob => blob.getName().toLowerCase().endsWith('.pptx'));
  if (!editionBlob || reportBlobs.length !== 1) {
    throw new Error('La edición debe contener publication.json y una única presentación PPTX.');
  }
  const edition = _validateEdition_(JSON.parse(editionBlob.getDataAsString('UTF-8')));
  const reportBlob = reportBlobs[0];
  if (String(edition.manifest.report || '') !== reportBlob.getName()) {
    throw new Error('La presentación no coincide con el manifiesto de la edición.');
  }

  const folder = _publicationFolder_();
  const timestamp = Utilities.formatDate(new Date(), 'Europe/Madrid', 'yyyyMMdd-HHmmss');
  const editionFile = folder.createFile(editionBlob.copyBlob().setName('publication-' + timestamp + '.json'));
  let reportFile;
  let slidesFileId = '';
  try {
    reportFile = Drive.Files.create({
      name: reportBlob.getName(),
      mimeType: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
      parents: [folder.getId()],
      appProperties: {npsLensArtifact: 'source-pptx'}
    }, reportBlob.copyBlob().setContentType('application/vnd.openxmlformats-officedocument.presentationml.presentation'), {
      supportsAllDrives: true,
      fields: 'id,name,mimeType,size'
    });
    const slides = Drive.Files.create({
      name: reportBlob.getName().replace(/\.pptx$/i, ''),
      mimeType: 'application/vnd.google-apps.presentation',
      parents: [folder.getId()],
      appProperties: {npsLensArtifact: 'newsletter-google-slides'}
    }, reportBlob.copyBlob().setContentType('application/vnd.openxmlformats-officedocument.presentationml.presentation'), {
      supportsAllDrives: true,
      fields: 'id,name,mimeType,webViewLink'
    });
    slidesFileId = String(slides.id || '');
    if (!slidesFileId || slides.mimeType !== 'application/vnd.google-apps.presentation') {
      throw new Error('Google Drive no pudo convertir la presentación a formato nativo.');
    }
    const nativeDeck = SlidesApp.openById(slidesFileId);
    if (!nativeDeck.getSlides().length) throw new Error('La presentación nativa no contiene diapositivas.');
    nativeDeck.saveAndClose();
    PropertiesService.getScriptProperties().setProperties({
      [NPS_LENS.editionFileProperty]: editionFile.getId(),
      [NPS_LENS.reportFileProperty]: slidesFileId
    });
  } catch (error) {
    editionFile.setTrashed(true);
    if (reportFile && reportFile.id) DriveApp.getFileById(reportFile.id).setTrashed(true);
    if (slidesFileId) DriveApp.getFileById(slidesFileId).setTrashed(true);
    throw error;
  }
  return getAdministration();
}

function getReportUrl() {
  const viewer = _viewer_();
  _assertViewer_(viewer);
  const fileId = _property_(NPS_LENS.reportFileProperty);
  return fileId ? DriveApp.getFileById(fileId).getUrl() : '';
}
