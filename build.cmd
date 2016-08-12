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

IF NOT EXIST %GLJPEGLIB%\libjpeg.lib (
    ECHO libjpeg.lib not found at %GLJPEGLIB%.
    ECHO Run setup.cmd before running build.cmd.
    GOTO BUILD_FAILED
)

SET INCLUDES=-I..\include -I..\src
SET LIBRARIES=User32.lib Gdi32.lib Shell32.lib Advapi32.lib Comdlg32.lib winmm.lib opengl32.lib %GLJPEGLIB%\*.lib

SET DEFINES_DEBUG=/D WINVER=0x0601 /D _WIN32_WINNT=0x0601 /D DEBUG /D _DEBUG /D UNICODE /D _UNICODE /D _STDC_FORMAT_MACROS /D _CRT_SECURE_NO_WARNINGS /D BUILD_STATIC
SET CPPFLAGS_DEBUG=%INCLUDES% /FC /nologo /W4 /WX /wd4505 /wd4611 /Zi /Fd%GLJPEGPDB% /EHsc /Od
SET LINKFLAGS_DEBUG=/MTd

SET DEFINES_RELEASE=/D WINVER=0x0601 /D _WIN32_WINNT=0x0601 /D UNICODE /D _UNICODE /D _STDC_FORMAT_MACROS /D _CRT_SECURE_NO_WARNINGS /D BUILD_STATIC
SET CPPFLAGS_RELEASE=%INCLUDES% /FC /nologo /W4 /WX /wd4505 /wd4611 /Zi /Fd%GLJPEGPDB% /EHsc /Ob2it
SET LINKFLAGS_RELEASE=/MT

IF [%1] NEQ [] (
    IF "%1" == "debug" (
        SET DEFINES=%DEFINES_DEBUG%
        SET CPPFLAGS=%CPPFLAGS_DEBUG%
        SET LNKFLAGS=%LINKFLAGS_DEBUG%
        ECHO Building debug configuration...
    ) ELSE (
        SET DEFINES=%DEFINES_RELEASE%
        SET CPPFLAGS=%CPPFLAGS_RELEASE%
        SET LNKFLAGS=%LINKFLAGS_RELEASE%
        ECHO Building release configuration - did you mean to specify "debug"?
    )
) ELSE (
    SET DEFINES=%DEFINES_RELEASE%
    SET CPPFLAGS=%CPPFLAGS_RELEASE%
    SET LNKFLAGS=%LINKFLAGS_RELEASE%
    ECHO Building release configuration...
)

IF NOT EXIST %GLJPEGOUT% MKDIR %GLJPEGOUT%

PUSHD %GLJPEGOUT%
cl %CPPFLAGS% ..\src\main.cc %DEFINES% %LIBRARIES% %LNKFLAGS% /Fe%GLJPEGEXE%
cl %CPPFLAGS% ..\src\dbenchst.cc %DEFINES% %LIBRARIES% %LNKFLAGS% /Fedbenchst.exe
cl %CPPFLAGS% ..\src\dbenchmt.cc %DEFINES% %LIBRARIES% %LNKFLAGS% /Fedbenchmt.exe
cl %CPPFLAGS% ..\src\decodemt.cc %DEFINES% %LIBRARIES% %LNKFLAGS% /Fedecodemt.exe
cl %CPPFLAGS% ..\src\decodemtm.cc %DEFINES% %LIBRARIES% %LNKFLAGS% /Fedecodemtm.exe
cl %CPPFLAGS% ..\src\decodemta.cc %DEFINES% %LIBRARIES% %LNKFLAGS% /Fedecodemta.exe
cl %CPPFLAGS% ..\src\decodemtu.cc %DEFINES% %LIBRARIES% %LNKFLAGS% /Fedecodemtu.exe
POPD

GOTO BUILD_COMPLETE

:BUILD_FAILED
@ECHO Build failed.
ENDLOCAL
EXIT /b

:BUILD_COMPLETE
@ECHO Build complete.
ENDLOCAL

