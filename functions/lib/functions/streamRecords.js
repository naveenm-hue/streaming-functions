"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.streamRecords = void 0;
const https_1 = require("firebase-functions/v2/https");
const MAX_SOURCES = 20;
const MAX_RECORDS = 10000;
const DELAY_MS = Number(process.env.STREAM_DELAY_MS ?? 0);
const sleep = (ms) => ms > 0 ? new Promise((r) => setTimeout(r, ms)) : Promise.resolve();
/**
 * Streaming Cloud Function.
 *
 * Client calls:
 *   const { stream, data } = await httpsCallable(functions, 'streamRecords').stream(payload);
 *   for await (const chunk of stream) { ... }
 *   const final = await data; // { status, totalRecords, elapsedMs }
 *
 * Each `chunk` is a StreamChunk:
 *   { type: 'progress',  stage: 'source_started' | 'source_done', source, sourceIndex, totalSources, ts }
 *   { type: 'message',   source, data, ts }
 *   { type: 'completed', totalRecords, elapsedMs, ts }
 *
 * Fatal errors are thrown as HttpsError and surface on the client
 * as an exception from the `for await` loop (wrap it in try/catch).
 */
exports.streamRecords = (0, https_1.onCall)({
    region: 'us-central1',
    memory: '512MiB',
    timeoutSeconds: 300,
    concurrency: 20,
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
        if (!source || typeof source.name !== 'string' || !source.name) {
            throw new https_1.HttpsError('invalid-argument', 'Each source must have a string "name".');
        }
        if (!Array.isArray(source.records)) {
            throw new https_1.HttpsError('invalid-argument', `Source "${source.name}" must have a "records" array.`);
        }
    }
    const totalInput = data.sources.reduce((sum, s) => sum + s.records.length, 0);
    if (totalInput > MAX_RECORDS) {
        throw new https_1.HttpsError('invalid-argument', `Too many records (max ${MAX_RECORDS}).`);
    }
    const streaming = request.acceptsStreaming && !!response;
    // True when the client has disconnected/cancelled.
    const isAborted = () => !!response?.signal?.aborted;
    // Sends a chunk only when the caller asked for streaming.
    // Returns false if the client is gone, so we can stop working.
    const emit = async (chunk) => {
        if (!streaming)
            return true;
        try {
            const ok = await response.sendChunk(chunk);
            return ok !== false;
        }
        catch {
            return false;
        }
    };
    // ─── Streaming Loop ───────────────────────────────────
    let messageCount = 0;
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
            if (isAborted() || !(await emit(startChunk))) {
                return { status: 'aborted', totalRecords: messageCount, elapsedMs: Date.now() - startTime };
            }
            for (const record of source.records) {
                if (isAborted()) {
                    return { status: 'aborted', totalRecords: messageCount, elapsedMs: Date.now() - startTime };
                }
                await sleep(DELAY_MS); // 0 in production; replace with real work (DB / API / LLM)
                const msgChunk = {
                    type: 'message',
                    source: source.name,
                    data: record,
                    ts: Date.now(),
                };
                if (!(await emit(msgChunk))) {
                    return { status: 'aborted', totalRecords: messageCount, elapsedMs: Date.now() - startTime };
                }
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
            if (!(await emit(doneChunk))) {
                return { status: 'aborted', totalRecords: messageCount, elapsedMs: Date.now() - startTime };
            }
        }
        const elapsedMs = Date.now() - startTime;
        const completed = {
            type: 'completed',
            totalRecords: messageCount,
            elapsedMs,
            ts: Date.now(),
        };
        await emit(completed);
        // Small summary only. Do NOT return all chunks, they were already streamed.
        return { status: 'ok', totalRecords: messageCount, elapsedMs };
    }
    catch (err) {
        if (err instanceof https_1.HttpsError)
            throw err;
        throw new https_1.HttpsError('internal', err?.message ?? 'stream error');
    }
});
//# sourceMappingURL=streamRecords.js.map