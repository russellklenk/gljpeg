@ECHO OFF

SET GLJPEGSRC="%~dp0src"
SET GLJPEGLIB="%~dp0lib"
SET GLJPEGOUT="%~dp0build"
SET GLJPEGEXE="%~dp0build\gljpeg.exe"
SET GLJPEGPDB="%~dp0build\gljpeg.pdb"

SETLOCAL
SET VSVERSION_2013=12.0
SET VSVERSION_2015=14.0
SET VSVERSION=%VSVERSION_2015%
IF NOT DEFINED DevEnvDir (
    CALL "C:\Program Files (x86)\Microsoft Visual Studio %VSVERSION%\VC\vcvarsall.bat" x86_amd64
)

start devenv /debugexe build\decodemt.exe
ENDLOCAL

