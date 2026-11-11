import { onCall, HttpsError } from 'firebase-functions/v2/https';
import type {
  StreamRequest,
  StreamChunk,
  ProgressChunk,
  MessageChunk,
  CompletedChunk,
  ErrorChunk,
} from '../lib/types.js';

// ─── Config ─────────────────────────────────────────────
const ARTIFICIAL_DELAY_MS = 300;
const MAX_SOURCES = 20;
const MAX_RECORDS = 10_000;

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

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
export const streamRecords = onCall(
  {
    region: 'us-central1',
    memory: '256MiB',
    timeoutSeconds: 300,
    concurrency: 40,
  },
  async (request, response) => {
    const startTime = Date.now();
    const data = request.data as StreamRequest;

    // ─── Validation ───────────────────────────────────────
    if (!data?.sources || !Array.isArray(data.sources) || data.sources.length === 0) {
      throw new HttpsError(
        'invalid-argument',
        'Request must include a non-empty "sources" array.'
      );
    }
    if (data.sources.length > MAX_SOURCES) {
      throw new HttpsError(
        'invalid-argument',
        `Too many sources (max ${MAX_SOURCES}).`
      );
    }

    for (const source of data.sources) {
      if (!source.name || typeof source.name !== 'string') {
        throw new HttpsError(
          'invalid-argument',
          'Each source must have a string "name".'
        );
      }
      if (!Array.isArray(source.records)) {
        throw new HttpsError(
          'invalid-argument',
          `Source "${source.name}" must have a "records" array.`
        );
      }
    }

    const totalRecords = data.sources.reduce(
      (sum, s) => sum + s.records.length,
      0
    );
    if (totalRecords > MAX_RECORDS) {
      throw new HttpsError(
        'invalid-argument',
        `Too many records (max ${MAX_RECORDS}).`
      );
    }

    // ─── Streaming Loop ───────────────────────────────────
    const allChunks: StreamChunk[] = [];
    let messageCount = 0;

    const emit = (chunk: StreamChunk): void => {
      if (request.acceptsStreaming && response) {
        response.sendChunk(chunk);
      }
      allChunks.push(chunk);
    };

    try {
      for (let i = 0; i < data.sources.length; i++) {
        const source = data.sources[i];

        const startChunk: ProgressChunk = {
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

          const msgChunk: MessageChunk = {
            type: 'message',
            source: source.name,
            data: record,
            ts: Date.now(),
          };
          emit(msgChunk);
          messageCount++;
        }

        const doneChunk: ProgressChunk = {
          type: 'progress',
          stage: 'source_done',
          source: source.name,
          sourceIndex: i,
          totalSources: data.sources.length,
          ts: Date.now(),
        };
        emit(doneChunk);
      }

      const completed: CompletedChunk = {
        type: 'completed',
        totalRecords: messageCount,
        elapsedMs: Date.now() - startTime,
        ts: Date.now(),
      };
      emit(completed);

      // Non-streaming fallback — returned to clients that don't use .stream()
      return {
        status: 'ok' as const,
        totalRecords: messageCount,
        elapsedMs: Date.now() - startTime,
        chunks: allChunks,
      };
    } catch (err: any) {
      const errChunk: ErrorChunk = {
        type: 'error',
        message: err?.message ?? 'unknown error',
        ts: Date.now(),
      };
      emit(errChunk);

      throw new HttpsError('internal', err?.message ?? 'stream error');
    }
  }
);
