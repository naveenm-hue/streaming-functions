import { onCall, HttpsError } from 'firebase-functions/v2/https';
import type {
  StreamRequest,
} from './types.js';

export const streamRecords = onCall(
  {
    region: 'us-central1',
    memory: '512MiB',
    timeoutSeconds: 300
  },
  async (request, response) => {
    const startTime = Date.now();
    const data = request.data as StreamRequest;
    const streaming = request.acceptsStreaming && !!response;

    const isAborted = (): boolean => !!response?.signal?.aborted;

    const emit = async (chunk:any): Promise<boolean> => {
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
        for (const record of source.records) {
          if (isAborted()) {
            return { status: 'aborted' as const, totalRecords: messageCount, elapsedMs: Date.now() - startTime };
          }
          const msgChunk = {
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

      }
      const elapsedMs = Date.now() - startTime;
      return { status: 'ok' as const, totalRecords: messageCount, elapsedMs };
    } catch (err: any) {
      if (err instanceof HttpsError) throw err;
      throw new HttpsError('internal', err?.message ?? 'stream error');
    }
  }
);