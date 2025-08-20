const fs = require('fs');
const { XMLParser } = require('fast-xml-parser');
const parser = new XMLParser();

// Leer canales de gatotv.com
const channelsXml = fs.readFileSync('sites/gatotv.com/channels.xml', 'utf8');
const parsed = parser.parse(channelsXml);

const channels = parsed.site.channels.channel.map(c => ({
  id: c['@_xmltv_id'],
  name: c['#text']
}));

// Programas de ejemplo
const programas = [
  { title: 'True Blood (S07 E02)', desc: 'Bill y Sookie enfrentan nuevas amenazas.' },
  { title: 'True Blood (S07 E03)', desc: 'La tensión aumenta en Bon Temps.' },
  { title: 'La vida inmortal de Henrietta Lacks', subTitle: 'Drama 2017', desc: 'Henrietta Lacks se convierte en pionera de la investigación médica.' },
  { title: 'Inception', subTitle: 'Acción 2010', desc: 'Un ladrón experto en el robo de secretos mediante los sueños recibe una última misión imposible.' }
];

// Fechas
const startDate = new Date();
startDate.setMinutes(0,0,0);
const endDate = new Date(startDate.getTime() + 72*60*60*1000);

let xml = '<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n';

// Agregar canales
channels.forEach(channel => {
  xml += `  <channel id="${channel.id}">\n    <display-name>${channel.name}</display-name>\n  </channel>\n`;
});

// Generar programación
channels.forEach(channel => {
  let currentTime = new Date(startDate);
  while(currentTime < endDate){
    programas.forEach(prog => {
      let start = formatDate(currentTime);
      currentTime.setMinutes(currentTime.getMinutes() + 60);
      let stop = formatDate(currentTime);

      xml += `  <programme start="${start}" stop="${stop}" channel="${channel.id}">\n`;
      xml += `    <title>${prog.title}</title>\n`;
      if(prog.subTitle) xml += `    <sub-title>${prog.subTitle}</sub-title>\n`;
      xml += `    <desc>${prog.desc}</desc>\n`;
      xml += `  </programme>\n`;
    });
  }
});

xml += '</tv>';
fs.writeFileSync('guide_custom.xml', xml, 'utf8');
console.log('guide_custom.xml generado con 72 horas de programación.');

function formatDate(d){
  return d.toISOString().replace(/[-:]/g,'').split('.')[0] + ' +0000';
}
