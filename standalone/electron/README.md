# Taiko Web Desktop (Fork)

## How to run (dev/prod)

### Development (use existing backend)
```bash
cd standalone/electron
ELECTRON_DEV=1 ELECTRON_BACKEND_URL=http://127.0.0.1:3123 npm run dev
```

### Development (spawn bundled backend)
```bash
cd standalone/electron
ELECTRON_DEV=1 npm run dev
```

### Production build
```bash
cd standalone/electron
npm run dist
```
