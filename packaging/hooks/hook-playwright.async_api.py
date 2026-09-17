from PyInstaller.utils.hooks import collect_data_files

# Bundle only the driver; never package cached/downloaded browser applications.
datas = collect_data_files("playwright", excludes=["**/.local-browsers/**"])
