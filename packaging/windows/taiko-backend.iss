#define MyAppName "taiko-web-backend"
#define MyVersion GetEnv("MyVersion")
#if MyVersion == ""
  #define MyVersion "dev"
#endif
#define MyDistDir GetEnv("MyDistDir")
#if MyDistDir == ""
  #define MyDistDir "dist\\backend\\taiko-web-backend"
#endif
; Sanitize version for filesystem/installer usage
#define MyVersionSafe StrReplace(StrReplace(StrReplace(StrReplace(MyVersion, "/", "-"), "\\", "-"), ":", "-"), " ", "-")

[Setup]
AppName={#MyAppName}
AppVersion={#MyVersionSafe}
DefaultDirName={userappdata}\{#MyAppName}
DisableDirPage=yes
DisableProgramGroupPage=yes
OutputDir=packaging\windows\Output
OutputBaseFilename=taiko-web-backend-setup-{#MyVersionSafe}
Compression=lzma2/max
SolidCompression=yes

[Files]
Source: "{#MyDistDir}\\*"; DestDir: "{app}"; Flags: recursesubdirs createallsubdirs ignoreversion

[Icons]
Name: "{autoprograms}\taiko-web-backend (desktop)"; Filename: "{app}\run_desktop.bat"
Name: "{autodesktop}\taiko-web-backend (desktop)"; Filename: "{app}\run_desktop.bat"

[Run]
Filename: "{app}\\run_desktop.bat"; Description: "Run taiko-web-backend (desktop)"; Flags: nowait postinstall skipifsilent

[Code]
function CurStepChanged(CurStep: TSetupStep);
var
  BatchFile: string;
begin
  if CurStep = ssInstall then begin
    BatchFile := ExpandConstant('{app}\\run_desktop.bat');
    SaveStringToFile(BatchFile,
      '@echo off'#13#10 +
      'set RUN_PROFILE=desktop'#13#10 +
      'start "" "%~dp0taiko-web-backend.exe" --port 8000'#13#10,
      False);
  end;
end;
