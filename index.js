import fs from "node:fs";
import fsp from "node:fs/promises";
import readline from "node:readline";
import path from "node:path";

// Utility functions
function getReplacement(location, reference, alternates) {
    if (location === ".") return "N";
    return location === "0" ? reference : alternates[location - 1];
}

function closeStream(stream) {
    return new Promise(res => {
        stream.once('close', res);
        stream.end();
    });
}

async function mkdirForce(path) {
    try {
        await fsp.mkdir(path);
    } catch(err) {
        if (err.code !== 'EEXIST') throw err;
    }
}

// Parse and validate arguments
const [
    nodePath, scriptPath,
    inputPath, outputName, ...sampleNames
] = process.argv;

if (process.argv.length < 5) {
    console.log(`Usage: ${nodePath} ${scriptPath} INPUT_PATH OUTPUT_NAME [SAMPLE_NAMES...]`);
    process.exit(1);
}

// Create temp directory
const tempDir = "temp";
await mkdirForce(tempDir);

// Create sample directory
const sampleDir = path.join(tempDir, outputName);
await mkdirForce(sampleDir);

// Open input file
const inputStream = fs.createReadStream(inputPath);
const inputLines = readline.createInterface({
    input: inputStream,
    crlfDelay: Infinity,
});

// Open temp files
const sampleFiles = sampleNames.map(n => path.join(sampleDir, n));
const sampleStreams = sampleFiles.map(f => fs.createWriteStream(f));

// Define state
let minPosition = 0;
let headers = null;
let lineNumber = -1;
let prevChromosome = "";
const sampleStreamDrainedPromises = sampleStreams.map(() => Promise.resolve());

for await (const line of inputLines) {
    // Print progress every 10000 lines
    if (++lineNumber % 10000 === 0)
        console.log("Processing line", lineNumber);

    // Skip comments
    if (line.startsWith("##")) continue;

    const columns = line.split('\t');
    if (headers == null) {
        // Parse header
        const lastColumnIndex = columns.length - 1;
        const sampleCount = lastColumnIndex - columns.indexOf("FORMAT");
        if (sampleNames.length !== sampleCount)
            throw new Error(`${sampleNames.length} samples expected but found ${sampleCount}.`);

        headers = columns;
        continue;
    }

    const namedColumns = {};
    for (let i = 0; i < columns.length; i++)
        namedColumns[headers[i]] = columns[i];

    const chromosome = namedColumns["#CHROM"];
    if (chromosome !== prevChromosome) {
        console.log("Entering chromosome", chromosome);

        // Reset position state
        minPosition = 0;
        prevChromosome = chromosome;
    }

    const position = parseInt(namedColumns["POS"]);
    if (position < minPosition) continue;

    const reference = namedColumns["REF"];
    const alternates = namedColumns["ALT"].split(',');
    minPosition = position + reference.length;

    const replacements = [];
    let maxReplacementLength = 0;

    const samples = columns.slice(-sampleNames.length);
    for (let i = 0; i < samples.length; i++) {
        const sample = samples[i];
        const location = /^(\d+|\.)/.exec(sample)[0];
        const replacement = getReplacement(location, reference, alternates);

        if (replacement.length > maxReplacementLength)
            maxReplacementLength = replacement.length;

        replacements[i] = replacement;
    }

    for (let i = 0; i < samples.length; i++) {
        const replacement = replacements[i];
        const stream = sampleStreams[i];

        // Wait for output stream to drain
        await sampleStreamDrainedPromises[i];
        if (!stream.write(replacement.padEnd(maxReplacementLength, "-")))
            sampleStreamDrainedPromises[i] = new Promise(res => {
                stream.once('drain', res);
            });
    }
}

// Close temp sample streams
await Promise.all(sampleStreams.map(closeStream));

console.log("Merging samples");

// Create output directory
const outputDir = "output";
await mkdirForce(outputDir);

// Open output file
const outputFile = path.join(outputDir, outputName);
const outputStream = fs.createWriteStream(outputFile);

// Merge temporary files
for (let i = 0; i < sampleNames.length; i++) {
    outputStream.write(`>${sampleNames[i]}\n`);

    const stream = fs.createReadStream(sampleFiles[i]);
    await new Promise((res, rej) => {
        stream.once('end', res);
        stream.once('error', rej);
        stream.pipe(outputStream, { end: false });
    });
    outputStream.write('\n');
}

// Close output stream
await closeStream(outputStream);

console.log("Deleting temporary files");

// Delete individual sample files
await fsp.rm(sampleDir, { recursive: true });
