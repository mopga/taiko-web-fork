# Desktop Asset Audit

This report lists launcher's asset files shipped with the desktop build (excluding anything under `public/`) and where they are referenced in the codebase. The audit was produced on 2025-11-07 and should be regenerated whenever assets are added or removed.

| Asset | Referenced From | Notes |
| --- | --- | --- |
| `assets/launcher/app.ico` | `standalone/electron/main.js`, `standalone/electron/package.json`, `packaging/windows/taiko-desktop.iss`, `taiko-web-backend.spec`, `tools/installer/app.iss` | Used for Windows executable icons and installers. |
| `assets/launcher/app.icns` | `standalone/electron/package.json` | Used as the macOS application icon. |
| `assets/launcher/title-screen.png` | `standalone/electron/renderer/splash.js`, `public/src/js/assets.js` | Splash screen background image. |
| `assets/launcher/dancing-don.gif` | `standalone/electron/renderer/splash.js`, `public/src/js/assets.js` | Splash screen mascot animation. |

No unreferenced launcher assets were found.
