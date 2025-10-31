#define AppVersion GetEnv('APP_VERSION', '0.1.0')

[Setup]
AppId={{A1E7A3C1-8F9E-4C65-B925-36216D0B5F1B}}
AppName=Taiko Web
AppVersion={#AppVersion}
DefaultDirName={autopf}\Taiko Web
DefaultGroupName=Taiko Web
PrivilegesRequired=admin
ArchitecturesInstallIn64BitMode=x64
DisableDirPage=no
AppendDefaultDirName=no
UsePreviousAppDir=yes
DirExistsWarning=yes
SetupIconFile=assets\launcher\app.ico
WizardStyle=modern
OutputDir=out
OutputBaseFilename=taiko-web-setup
Compression=lzma2
SolidCompression=yes

[Files]
Source: "standalone\electron\dist\win-unpacked\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs
Source: "assets\launcher\app.ico"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\Taiko Web"; Filename: "{app}\Taiko Web.exe"; WorkingDir: "{app}"; IconFilename: "{app}\app.ico"
Name: "{commondesktop}\Taiko Web"; Filename: "{app}\Taiko Web.exe"; WorkingDir: "{app}"; IconFilename: "{app}\app.ico"

[Run]
Filename: "{app}\\Taiko Web.exe"; Description: "Запустить Taiko Web"; Flags: nowait postinstall skipifsilent
