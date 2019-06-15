const fs = require('fs');
const { exec } = require('child_process');
const OUT = 'out';

const sortFiles = () => {
  console.log('Sorting files...')
  const folderName = `./${OUT}`;
  fs.readdirSync(folderName).forEach((file) => {
    const filepath = `out/${file}`;
    const cmd = `sort -t, -k 2.7,2.10n -k 2.4,2.5n -k 2.1,2.2n -k 2.12,2.13n -k 2.15,2.16n -k 2.18,2.19n ${filepath} -o ${filepath}`;
    console.log('Executing Command', cmd);
    exec(cmd, (err, stdout, stderr) => {
      if (err) {
        return;
      }
      stderr && console.log(`stdout: ${stdout}`);
      stderr && console.log(`stderr: ${stderr}`);
    });
  });
  console.log('Sorting DONE.');
}

const start = process.hrtime();
console.log('Starting Sort Job..', new Date());
sortFiles();
const end = process.hrtime(start);
console.info('Execution time: %ds %dms', end[0], end[1] / 1000000);