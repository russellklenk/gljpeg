@ECHO OFF

SETLOCAL
SET GLJPEG3RD="%~dp03rdparty"
SET GLJPEGINC="%~dp0include"
SET GLJPEGSRC="%~dp0src"
SET GLJPEGLIB="%~dp0lib"
SET GLJPEGOUT="%~dp0build"
SET GLJPEGEXE="%~dp0build\gljpeg.exe"
SET GLJPEGPDB="%~dp0build\gljpeg.pdb"

ECHO Removing libjpeg header files from %GLJPEGINC%...
IF EXIST %GLJPEGINC%\jpeglib.h DEL %GLJPEGINC%\jpeglib.h
IF EXIST %GLJPEGINC%\jconfig.h DEL %GLJPEGINC%\jconfig.h
IF EXIST %GLJPEGINC%\jmorecfg.h DEL %GLJPEGINC%\jmorecfg.h
IF EXIST %GLJPEGINC%\jerror.h DEL %GLJPEGINC%\jerror.h

IF EXIST %GLJPEGLIB% (
    ECHO Removing directory %GLJPEGLIB%...
    DEL %GLJPEGLIB%\*.lib
    RMDIR %GLJPEGLIB%
)

IF EXIST %GLJPEGOUT% (
    ECHO Removing directory %GLJPEGOUT%...
    RMDIR /S /Q %GLJPEGOUT%
)

IF EXIST %GLJPEG3RD%\build (
    ECHO Removing directory %GLJPEG3RD%\build...
    RMDIR /S /Q %GLJPEG3RD%\build
)

ENDLOCAL

ECHO Re-run setup.cmd and then build.cmd to rebuild everything.

