; Hybrid installer definition used for backward compatibility.
; WARNING: this script exists only so automated pipelines that still
; reference the historical backend installer keep working. Do not run
; this installer manually; prefer taiko-desktop.iss instead.
; When a full desktop staging directory is provided, delegate to the
; desktop installer implementation. Otherwise fall back to the legacy
; backend-only installer behavior without creating launchers or batch
; files so the payload cannot be mistaken for the desktop product.

#define BACKEND_APP_NAME "taiko-web-backend"

#ifdef MyVersion
  #define RAW_BACKEND_VERSION MyVersion
#else
  #define RAW_BACKEND_VERSION GetEnv("MyVersion")
#endif
#if RAW_BACKEND_VERSION == ""
  #define RAW_BACKEND_VERSION "dev"
#endif
#define BACKEND_VERSION RAW_BACKEND_VERSION
#define BACKEND_VERSION_SAFE StringChange(StringChange(StringChange(StringChange(BACKEND_VERSION, "/", "-"), "\\", "-"), ":", "-"), " ", "-")

#ifdef MyDistDir
  #define BACKEND_SOURCE_ROOT MyDistDir
#else
  #define BACKEND_SOURCE_ENV GetEnv("MyDistDir")
  #if BACKEND_SOURCE_ENV == ""
    #define BACKEND_SOURCE_ROOT "..\\..\\dist\\backend\\taiko-web-backend"
  #else
    #define BACKEND_SOURCE_ROOT BACKEND_SOURCE_ENV
  #endif
#endif

#define DESKTOP_GUESS_ROOT ExtractFileDir(ExtractFileDir(BACKEND_SOURCE_ROOT)) + "\\win-unpacked"

#ifexist DESKTOP_GUESS_ROOT + "\\Taiko Web Desktop.exe"
  #define DESKTOP_VERSION_OVERRIDE BACKEND_VERSION
  #define DESKTOP_SOURCE_OVERRIDE DESKTOP_GUESS_ROOT
  #ifdef MyVersion
    #undef MyVersion
  #endif
  #ifdef MyDistDir
    #undef MyDistDir
  #endif
  #include "taiko-desktop.iss"
#else

[Setup]
AppName={#BACKEND_APP_NAME}
AppVersion={#BACKEND_VERSION_SAFE}
DefaultDirName={userappdata}\{#BACKEND_APP_NAME}
DisableDirPage=yes
DisableProgramGroupPage=yes
OutputDir={#SourcePath}\Output
OutputBaseFilename=taiko-web-backend-setup-{#BACKEND_VERSION_SAFE}
Compression=lzma2/max
SolidCompression=yes

[Files]
Source: "{#BACKEND_SOURCE_ROOT}\\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs ignoreversion

#endif
