<?php
// Archivo XML de los canales
$xmlFile = __DIR__ . '/Tempest-EPG-Generator/Siteconfigs/Multi Nation/[ENC][EX]/mi.tv_1.channel.xml';

// Verificar que existe
if (!file_exists($xmlFile)) {
    die("Error: no se encontró el archivo XML en $xmlFile\n");
}

// Cargar el XML
$xml = simplexml_load_file($xmlFile);

// Abrir archivo para guardar listado
$outputFile = __DIR__ . '/channels_list.txt';
$fh = fopen($outputFile, 'w');
if (!$fh) {
    die("Error: no se pudo crear $outputFile\n");
}

echo "Listado de canales:\n\n";

// Recorrer los canales
foreach ($xml->channel as $channel) {
    $id = (string) $channel['id'];
    $name = (string) $channel->{'display-name'};
    $line = "$id - $name\n";
    echo $line;
    fwrite($fh, $line);
}

fclose($fh);
echo "\nArchivo guardado en $outputFile\n";
