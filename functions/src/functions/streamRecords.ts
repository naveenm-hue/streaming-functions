import { onCall, HttpsError } from 'firebase-functions/v2/https';
import type {
  StreamRequest,
  StreamChunk,
  ProgressChunk,
  MessageChunk,
  CompletedChunk,
} from '../lib/types.js';

const MAX_SOURCES = 20;
const MAX_RECORDS = 10_000;
const DELAY_MS = Number(process.env.STREAM_DELAY_MS ?? 0);

const sleep = (ms: number): Promise<void> =>
  ms > 0 ? new Promise<void>((r) => setTimeout(r, ms)) : Promise.resolve();

export const streamRecords = onCall(
  {
    region: 'us-central1',
    memory: '512MiB',
    timeoutSeconds: 300,
    concurrency: 20,
  },
  async (request, response) => {
    const startTime = Date.now();
    const data = request.data as StreamRequest;

     if (!data?.sources || !Array.isArray(data.sources) || data.sources.length === 0) {
      throw new HttpsError(
        'invalid-argument',
        'Request must include a non-empty "sources" array.'
      );
    }
    if (data.sources.length > MAX_SOURCES) {
      throw new HttpsError('invalid-argument', `Too many sources (max ${MAX_SOURCES}).`);
    }

    for (const source of data.sources) {
      if (!source || typeof source.name !== 'string' || !source.name) {
        throw new HttpsError('invalid-argument', 'Each source must have a string "name".');
      }
      if (!Array.isArray(source.records)) {
        throw new HttpsError(
          'invalid-argument',
          `Source "${source.name}" must have a "records" array.`
        );
      }
    }

    const totalInput = data.sources.reduce((sum, s) => sum + s.records.length, 0);
    if (totalInput > MAX_RECORDS) {
      throw new HttpsError('invalid-argument', `Too many records (max ${MAX_RECORDS}).`);
    }

    const streaming = request.acceptsStreaming && !!response;

    const isAborted = (): boolean => !!response?.signal?.aborted;

    const emit = async (chunk: StreamChunk): Promise<boolean> => {
      if (!streaming) return true;
      try {
        const ok = await response!.sendChunk(chunk);
        return ok !== false;
      } catch {
        return false;
      }
    };
    let messageCount = 0;

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
        if (isAborted() || !(await emit(startChunk))) {
          return { status: 'aborted' as const, totalRecords: messageCount, elapsedMs: Date.now() - startTime };
        }

        for (const record of source.records) {
          if (isAborted()) {
            return { status: 'aborted' as const, totalRecords: messageCount, elapsedMs: Date.now() - startTime };
          }

          await sleep(DELAY_MS); 

          const msgChunk: MessageChunk = {
            type: 'message',
            source: source.name,
            data: record,
            ts: Date.now(),
          };
          if (!(await emit(msgChunk))) {
            return { status: 'aborted' as const, totalRecords: messageCount, elapsedMs: Date.now() - startTime };
          }
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
        if (!(await emit(doneChunk))) {
          return { status: 'aborted' as const, totalRecords: messageCount, elapsedMs: Date.now() - startTime };
        }
      }

      const elapsedMs = Date.now() - startTime;

      const completed: CompletedChunk = {
        type: 'completed',
        totalRecords: messageCount,
        elapsedMs,
        ts: Date.now(),
      };
      await emit(completed);

      return { status: 'ok' as const, totalRecords: messageCount, elapsedMs };
    } catch (err: any) {
      if (err instanceof HttpsError) throw err;
      throw new HttpsError('internal', err?.message ?? 'stream error');
    }
  }
);