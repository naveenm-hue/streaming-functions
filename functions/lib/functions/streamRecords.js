"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.streamRecords = void 0;
const https_1 = require("firebase-functions/v2/https");
// ─── Config ─────────────────────────────────────────────
const ARTIFICIAL_DELAY_MS = 300;
const MAX_SOURCES = 20;
const MAX_RECORDS = 10000;
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
/**
 * Streaming Cloud Function.
 *
 * Client calls:
 *   const { stream } = await httpsCallable(functions, 'streamRecords').stream(payload);
 *   for await (const chunk of stream) { ... }
 *
 * Each `chunk` is a StreamChunk:
 *   { type: 'progress',  stage: 'source_started' | 'source_done', source, sourceIndex, totalSources, ts }
 *   { type: 'message',   source, data, ts }
 *   { type: 'completed', totalRecords, elapsedMs, ts }
 *   { type: 'error',     message, ts }
 */
exports.streamRecords = (0, https_1.onCall)({
    region: 'us-central1',
    memory: '256MiB',
    timeoutSeconds: 300,
    concurrency: 40,
}, async (request, response) => {
    const startTime = Date.now();
    const data = request.data;
    // ─── Validation ───────────────────────────────────────
    if (!data?.sources || !Array.isArray(data.sources) || data.sources.length === 0) {
        throw new https_1.HttpsError('invalid-argument', 'Request must include a non-empty "sources" array.');
    }
    if (data.sources.length > MAX_SOURCES) {
        throw new https_1.HttpsError('invalid-argument', `Too many sources (max ${MAX_SOURCES}).`);
    }
    for (const source of data.sources) {
        if (!source.name || typeof source.name !== 'string') {
            throw new https_1.HttpsError('invalid-argument', 'Each source must have a string "name".');
        }
        if (!Array.isArray(source.records)) {
            throw new https_1.HttpsError('invalid-argument', `Source "${source.name}" must have a "records" array.`);
        }
    }
    const totalRecords = data.sources.reduce((sum, s) => sum + s.records.length, 0);
    if (totalRecords > MAX_RECORDS) {
        throw new https_1.HttpsError('invalid-argument', `Too many records (max ${MAX_RECORDS}).`);
    }
    // ─── Streaming Loop ───────────────────────────────────
    const allChunks = [];
    let messageCount = 0;
    const emit = (chunk) => {
        if (request.acceptsStreaming && response) {
            response.sendChunk(chunk);
        }
        allChunks.push(chunk);
    };
    try {
        for (let i = 0; i < data.sources.length; i++) {
            const source = data.sources[i];
            const startChunk = {
                type: 'progress',
                stage: 'source_started',
                source: source.name,
                sourceIndex: i,
                totalSources: data.sources.length,
                ts: Date.now(),
            };
            emit(startChunk);
            for (const record of source.records) {
                await sleep(ARTIFICIAL_DELAY_MS);
                const msgChunk = {
                    type: 'message',
                    source: source.name,
                    data: record,
                    ts: Date.now(),
                };
                emit(msgChunk);
                messageCount++;
            }
            const doneChunk = {
                type: 'progress',
                stage: 'source_done',
                source: source.name,
                sourceIndex: i,
                totalSources: data.sources.length,
                ts: Date.now(),
            };
            emit(doneChunk);
        }
        const completed = {
            type: 'completed',
            totalRecords: messageCount,
            elapsedMs: Date.now() - startTime,
            ts: Date.now(),
        };
        emit(completed);
        // Non-streaming fallback — returned to clients that don't use .stream()
        return {
            status: 'ok',
            totalRecords: messageCount,
            elapsedMs: Date.now() - startTime,
            chunks: allChunks,
        };
    }
    catch (err) {
        const errChunk = {
            type: 'error',
            message: err?.message ?? 'unknown error',
            ts: Date.now(),
        };
        emit(errChunk);
        throw new https_1.HttpsError('internal', err?.message ?? 'stream error');
    }
});
//# sourceMappingURL=streamRecords.js.map