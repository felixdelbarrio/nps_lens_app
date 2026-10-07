"""Preserve directories used by python-pptx to resolve its XML templates."""

from PyInstaller.utils.hooks import collect_data_files

datas = collect_data_files("pptx")
module_collection_mode = {"pptx.oxml": "pyz+py"}
