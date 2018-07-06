from distutils.core import setup
import py2exe

# import numpy
import os
import sys


# add any numpy directory containing a dll file to sys.path
# def numpy_dll_paths_fix():
#     """
#     This method was sourced from:
#             https://stackoverflow.com/questions/36191770/py2exe-errno-2-no-such-file-or-directory-numpy-atlas-dll
#     """
#     paths = set()
#     np_path = numpy.__path__[0]
#     for dirpath, _, filenames in os.walk(np_path):
#         for item in filenames:
#             if item.endswith('.dll'):
#                 paths.add(dirpath)
#
#     sys.path.append(*list(paths))
#
#
# numpy_dll_paths_fix()


manifest = """
<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<assembly xmlns="urn:schemas-microsoft-com:asm.v1"
manifestVersion="1.0">
<assemblyIdentity
    version="0.64.1.0"
    processorArchitecture="x86"
    name="Controls"
    type="win32"
/>
<description>H2OUtility</description>
<dependency>
    <dependentAssembly>
        <assemblyIdentity
            type="win32"
            name="Microsoft.Windows.Common-Controls"
            version="6.0.0.0"
            processorArchitecture="X86"
            publicKeyToken="6595b64144ccf1df"
            language="*"
        />
    </dependentAssembly>
</dependency>
</assembly>
"""

"""
installs manifest and icon into the .exe
but icon is still needed as we open it
for the window icon (not just the .exe)
changelog and logo are included in dist
"""

setup(
    windows = [
        {
            "script": "VisualUpdater.py",
            # "icon_resources": [(1, "yourapplication.ico")],
            "other_resources": [(24,1,manifest)]
        }
    ],
    # options={
    #     "py2exe": {
    #         "dll_excludes": ["MSVCP90.dll"]
    #     }
    # }
      # data_files=["yourapplication.ico"]
)