"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.streamRecords = void 0;
const https_1 = require("firebase-functions/v2/https");
exports.streamRecords = (0, https_1.onCall)({
    region: 'us-central1',
    memory: '512MiB',
    timeoutSeconds: 300
}, async (request, response) => {
    const startTime = Date.now();
    const data = request.data;
    const streaming = request.acceptsStreaming && !!response;
    const isAborted = () => !!response?.signal?.aborted;
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
    let messageCount = 0;
    try {
        for (let i = 0; i < data.sources.length; i++) {
            const source = data.sources[i];
            for (const record of source.records) {
                if (isAborted()) {
                    return { status: 'aborted', totalRecords: messageCount, elapsedMs: Date.now() - startTime };
                }
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
        }
        const elapsedMs = Date.now() - startTime;
        return { status: 'ok', totalRecords: messageCount, elapsedMs };
    }
    catch (err) {
        if (err instanceof https_1.HttpsError)
            throw err;
        throw new https_1.HttpsError('internal', err?.message ?? 'stream error');
    }
});
//# sourceMappingURL=streamRecords.js.map