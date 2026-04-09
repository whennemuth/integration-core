const fs = require('fs');
const path = require('path');

function deleteDir(dirPath) {
  if (fs.existsSync(dirPath)) {
    fs.rmSync(dirPath, { recursive: true, force: true });
    console.log(`Removed: ${dirPath}`);
  }
}

// Remove test directories from dist
deleteDir(path.join(__dirname, '../dist/esm/test'));
deleteDir(path.join(__dirname, '../dist/cjs/test'));
deleteDir(path.join(__dirname, '../dist/types/test'));
deleteDir(path.join(__dirname, '../dist/types/src/test'));

console.log('Test directories removed from dist');
