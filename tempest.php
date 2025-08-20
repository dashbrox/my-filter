<?php
// tempest.php - Simulación de generación de EPG

$options = getopt("", ["engine:", "createxmlgz:", "output:"]);

$engine = $options['engine'] ?? null;
$createxmlgz = $options['createxmlgz'] ?? null;
$output = $options['output'] ?? 'guide.xml';

if ($engine !== 'Generate') {
    echo "Error: engine no soportado\n";
    exit(1);
}

// Simulamos contenido XML básico
$xmlContent = <<<XML
<?xml version="1.0" encoding="UTF-8"?>
<tv>
    <channel id="Canal1">
        <display-name>Canal 1</display-name>
    </channel>
    <programme channel="Canal1" start="20250819060000 +0000" stop="20250819070000 +0000">
        <title>Película de prueba</title>
        <desc>Descripción de ejemplo</desc>
    </programme>
</tv>
XML;

// Guardar XML
file_put_contents($output, $xmlContent);
echo "Archivo '$output' generado correctamente.\n";

// Opcional: generar .gz si se pidió
if ($createxmlgz === 'on') {
    $gzFile = $output . '.gz';
    $gz = gzopen($gzFile, 'w9');
    gzwrite($gz, $xmlContent);
    gzclose($gz);
    echo "Archivo comprimido '$gzFile' generado correctamente.\n";
}

exit(0);
