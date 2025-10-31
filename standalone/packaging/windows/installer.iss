#ifndef MyAppVersion
  #define MyAppVersion "dev"
#endif

[Setup]
AppName=taiko-web-backend
AppVersion={#MyAppVersion}
DefaultDirName={autopf64}\taiko-web-backend
PrivilegesRequired=admin
ArchitecturesInstallIn64BitMode=x64
DisableDirPage=no
UsePreviousAppDir=yes
DirExistsWarning=yes
SetupIconFile=..\..\..\assets\launcher\app.ico
WizardStyle=modern
OutputDir=out
OutputBaseFilename=taiko-web-backend-setup-{#MyAppVersion}
Compression=lzma2/max
SolidCompression=yes

[Files]
Source: "standalone\dist\backend\taiko-web-backend\*"; DestDir: "{app}"; Flags: recursesubdirs ignoreversion
Source: "standalone\electron\dist\win-unpacked\*"; DestDir: "{app}\electron"; Flags: recursesubdirs ignoreversion
Source: "..\..\..\assets\launcher\app.ico"; DestDir: "{app}"; Flags: ignoreversion

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"

[Icons]
Name: "{group}\Taiko Web Backend"; Filename: "{app}\taiko-web-backend.exe"; IconFilename: "{app}\app.ico"
Name: "{commondesktop}\Taiko Web Backend"; Filename: "{app}\taiko-web-backend.exe"; Tasks: desktopicon; IconFilename: "{app}\app.ico"
