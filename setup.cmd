@ECHO OFF

SETLOCAL
SET GLJPEG3RD="%~dp03rdparty"
SET GLJPEGINC="%~dp0include"
SET GLJPEGSRC="%~dp0src"
SET GLJPEGLIB="%~dp0lib"
SET GLJPEGOUT="%~dp0build"
SET GLJPEGEXE="%~dp0build\gljpeg.exe"
SET GLJPEGPDB="%~dp0build\gljpeg.pdb"

IF NOT EXIST %GLJPEGLIB% MKDIR %GLJPEGLIB%
IF NOT EXIST %GLJPEGOUT% MKDIR %GLJPEGOUT%

ECHO Building 3rdparty dependency libjpeg...
CALL "build-libjpeg.cmd" %1

ECHO Copying libjpeg headers to %GLJPEGINC%...
COPY %LIBJPEGSRC%\jpeglib.h %GLJPEGINC%\
COPY %LIBJPEGSRC%\jconfig.h %GLJPEGINC%\
COPY %LIBJPEGSRC%\jmorecfg.h %GLJPEGINC%\
COPY %LIBJPEGSRC%\jerror.h %GLJPEGINC%\

ECHO Copying libjpeg outputs to %GLJPEGLIB%...
COPY %LIBJPEGLIB% %GLJPEGLIB%

ECHO Copying libjpeg debug information to %GLJPEGOUT%...
COPY %LIBJPEGPDB% %GLJPEGOUT%\

ECHO Ready to run build.cmd.

ENDLOCAL

