#define APP_NAME "Taiko Web Desktop"
#define APP_ID "com.taiko.web.desktop"
#ifdef MyVersion
  #define RAW_APP_VERSION MyVersion
#else
  #define RAW_APP_VERSION GetEnv("TAIKO_DESKTOP_VERSION")
#endif
#if RAW_APP_VERSION == ""
  #define RAW_APP_VERSION "dev"
#endif
#define APP_VERSION RAW_APP_VERSION
#define APP_VERSION_SAFE StringChange(StringChange(StringChange(StringChange(APP_VERSION, "/", "-"), "\\", "-"), ":", "-"), " ", "-")
#ifdef MyDistDir
  #define SOURCE_DIR MyDistDir
#else
  #define SOURCE_DIR_ENV GetEnv("DESKTOP_STAGING_DIR")
  #if SOURCE_DIR_ENV == ""
    #define SOURCE_DIR "..\\..\\standalone\\electron\\dist\\win-unpacked"
  #else
    #define SOURCE_DIR SOURCE_DIR_ENV
  #endif
#endif
#define OUTPUT_DIR_ENV GetEnv("TAIKO_INSTALLER_OUTPUT")
#if OUTPUT_DIR_ENV == ""
  #define OUTPUT_DIR "..\\..\\standalone\\electron\\dist\\installer"
#else
  #define OUTPUT_DIR OUTPUT_DIR_ENV
#endif
#define LAUNCHER_ICON "..\\..\\assets\\launcher\\app.ico"
#define MAIN_EXE "Taiko Web Desktop.exe"
#define BACKEND_EXE "backend\\taiko-web-backend.exe"

#ifnexist LAUNCHER_ICON
  #error "Launcher icon not found at " + LAUNCHER_ICON
#endif
#ifnexist SOURCE_DIR + "\\" + MAIN_EXE
  #error "Main executable missing at " + SOURCE_DIR + "\\" + MAIN_EXE
#endif
#ifnexist SOURCE_DIR + "\\" + BACKEND_EXE
  #error "Backend executable missing at " + SOURCE_DIR + "\\" + BACKEND_EXE
#endif

[Setup]
AppId={#APP_ID}
AppName={#APP_NAME}
AppVersion={#APP_VERSION_SAFE}
AppPublisher=Taiko Web
DefaultDirName={autopf}\Taiko Web Desktop
DisableDirPage=no
DisableProgramGroupPage=yes
PrivilegesRequired=admin
PrivilegesRequiredOverridesAllowed=commandline
OutputDir={#OUTPUT_DIR}
OutputBaseFilename=TaikoWebDesktopSetup-{#APP_VERSION_SAFE}
Compression=lzma2/max
SolidCompression=yes
SetupIconFile={#LAUNCHER_ICON}
UninstallDisplayIcon={app}\assets\launcher\app.ico
ArchitecturesAllowed=x64
ArchitecturesInstallIn64BitMode=x64

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Files]
Source: "{#SOURCE_DIR}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{autoprograms}\Taiko Web Desktop"; Filename: "{app}\{#MAIN_EXE}"; WorkingDir: "{app}"; IconFilename: "{app}\assets\launcher\app.ico"
Name: "{autodesktop}\Taiko Web Desktop"; Filename: "{app}\{#MAIN_EXE}"; WorkingDir: "{app}"; IconFilename: "{app}\assets\launcher\app.ico"

[Run]
Filename: "{app}\{#MAIN_EXE}"; Description: "Launch Taiko Web Desktop"; Flags: nowait postinstall skipifsilent

[UninstallDelete]
Type: filesandordirs; Name: "{app}"
