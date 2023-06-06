# -*- mode: python ; coding: utf-8 -*-

import os
import sys

a = Analysis(['GUI.py'],
             pathex=[],
             binaries=[],
             hiddenimports=[],
             hookspath=[],
             hooksconfig={},
             runtime_hooks=[],
             excludes=[],
             win_no_prefer_redirects=False,
             win_private_assemblies=False,
             noarchive=False)

pyz = PYZ(a.pure, a.zipped_data)

exe = EXE(pyz,
          a.scripts,
          [],
          exclude_binaries=True,
          name='jsonaut',
          debug=False,
          bootloader_ignore_signals=False,
          strip=False,
          upx=True,
          console=False,
          icon='jsonaut.ico',
          disable_windowed_traceback=False,
          target_arch=None,
          codesign_identity=None,
          entitlements_file=None)

coll = COLLECT(exe,
               a.binaries,
               a.zipfiles,
               a.datas,
               strip=False,
               upx=True,
               upx_exclude=[],
               name='jsonaut',
               pathex=['dist'],  # Set the output directory to 'dist'
               upx_exclude_dll=['api-ms-win-*'])  # Exclude specific DLLs from UPX compression
