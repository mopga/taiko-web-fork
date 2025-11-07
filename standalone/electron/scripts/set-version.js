const fs = require('fs');
const path = require('path');

function writeJson(filePath, updater) {
  const absolutePath = path.resolve(filePath);
  const payload = JSON.parse(fs.readFileSync(absolutePath, 'utf8'));
  const updated = updater(payload);
  fs.writeFileSync(absolutePath, `${JSON.stringify(updated, null, 2)}\n`, 'utf8');
}

function updatePackageJson(version) {
  const packageFile = path.join(__dirname, '..', 'package.json');
  writeJson(packageFile, (pkg) => {
    const next = { ...pkg };
    next.version = version;
    return next;
  });
}

function updatePackageLock(version) {
  const lockFile = path.join(__dirname, '..', 'package-lock.json');
  if (!fs.existsSync(lockFile)) {
    return;
  }
  writeJson(lockFile, (lock) => {
    const next = { ...lock };
    next.version = version;
    if (next.packages && typeof next.packages === 'object') {
      const packages = { ...next.packages };
      const rootPackage = { ...(packages[''] || {}) };
      rootPackage.version = version;
      packages[''] = rootPackage;
      next.packages = packages;
    }
    return next;
  });
}

function main() {
  const [, , argVersion] = process.argv;
  const version = (argVersion || process.env.DESKTOP_APP_VERSION || '').trim();
  if (!version) {
    console.error('Expected version as an argument or DESKTOP_APP_VERSION env var');
    process.exit(1);
  }
  updatePackageJson(version);
  updatePackageLock(version);
}

if (require.main === module) {
  main();
}
