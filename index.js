import fs from "node:fs";
import fsp from "node:fs/promises";
import readline from "node:readline";
import path from "node:path";

// Utility functions
function getReplacement(location, reference, alternates) {
    if (location === ".") return null;
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
const sampleCount = sampleNames.length;

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
let window = [];
let windowEnd = 0;
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
        const actualSampleCount = lastColumnIndex - columns.indexOf("FORMAT");
        if (sampleCount !== actualSampleCount)
            throw new Error(`${sampleNames.length} samples expected but found ${actualSampleCount}.`);

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
        windowEnd = 0;
        prevChromosome = chromosome;
    }

    const position = parseInt(namedColumns["POS"]);
    if (position >= windowEnd) {
        for (const replacements of window) {
            let maxReplacementLength = 0;
            for (let si = 0; si < sampleCount; si++) {
                const {length} = replacements[si] ??= "N";
                if (length > maxReplacementLength)
                    maxReplacementLength = length;
            }

            for (let si = 0; si < sampleCount; si++) {
                const replacement = replacements[si].padEnd(maxReplacementLength, "-");
                const stream = sampleStreams[si];

                // Wait for output stream to drain
                await sampleStreamDrainedPromises[si];
                if (!stream.write(replacement))
                    sampleStreamDrainedPromises[si] = new Promise(res => {
                        stream.once('drain', res);
                    });
            }
        }

        window = [];
        windowEnd = position;
    }

    // Expand window to fit max referenced position
    const reference = namedColumns["REF"];
    const minWindowEnd = position + reference.length;
    for (let pos = windowEnd; pos < minWindowEnd; pos++)
        window.push(new Array(sampleCount));

    // Update window end position
    if (minWindowEnd > windowEnd)
        windowEnd = minWindowEnd;

    // Calculate position in window
    const windowStart = windowEnd - window.length;
    const windowPos = position - windowStart;

    const alternates = namedColumns["ALT"].split(',');
    const samples = columns.slice(-sampleCount);
    for (let si = 0; si < sampleCount; si++) {
        if (window[windowPos][si] != null) continue;

        const sample = samples[si];
        const location = /^(\d+|\.)/.exec(sample)[0];
        const replacement = getReplacement(location, reference, alternates);
        if (replacement == null) continue;

        if (replacement.length >= reference.length) {
            window[windowPos][si] = replacement;
            continue;
        }

        for (let i = 0; i < replacement.length; i++)
            window[windowPos + i][si] = replacement[i];

        for (let i = replacement.length; i < reference.length; i++)
            window[windowPos + i][si] = '-';
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
