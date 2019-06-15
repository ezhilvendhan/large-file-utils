const fs = require('fs');
const readLine = require('readline');
const path = require('path');
const { exec } = require('child_process');
const OUT = 'out';

const tags = new Set();

const processFile = (fileName) => {
  console.log(fileName);

  const processLine = (line) => {
    if(!line) return;
    const _tag = line.split(',')[0];
    const tagFileName = path.join(__dirname, OUT, `${_tag}.csv`);
    const lineWithNewLine = line + "\n";
    let file = null;
    if(tags.has(_tag)) {
      fs.appendFileSync(tagFileName, lineWithNewLine);
    } else {
      try {
        fs.writeFileSync(tagFileName, lineWithNewLine);
      } catch(err) {
        console.log(`Error while creating ${tagFileName}`, err);
      }
      tags.add(_tag);
    }
  };

  var lineReader = require('readline').createInterface({
    input: require('fs').createReadStream(fileName)
  });
  
  lineReader.on('line', function (line) {
    processLine(line);
  });
};


const main = (maxIdx) => {
  var dir = `./${OUT}`;
  if (!fs.existsSync(dir)){
    fs.mkdirSync(dir);
  }
  for(let i = 1; i <= maxIdx; i++) {
    const folderName = `./${i}/`;
    if (fs.existsSync(folderName)) {
      fs.readdirSync(folderName).forEach((file) => {
        processFile(`${folderName}${file}`);
      });
    } else break;
  }
}

const start = process.hrtime();
console.log('Starting Main Job..', new Date());
main(10);
const end = process.hrtime(start);
console.info('Execution time: %ds %dms', end[0], end[1] / 1000000);