
export interface SourceInput {
  name: string;
  records: unknown[];
}

export interface StreamRequest {
  sources: SourceInput[];
}

export interface ProgressChunk {
  type: 'progress';
  stage: 'source_started' | 'source_done';
  source: string;
  sourceIndex: number;
  totalSources: number;
  ts: number;
}

export interface MessageChunk {
  type: 'message';
  source: string;
  data: unknown;
  ts: number;
}

export interface CompletedChunk {
  type: 'completed';
  totalRecords: number;
  elapsedMs: number;
  ts: number;
}

export interface ErrorChunk {
  type: 'error';
  message: string;
  ts: number;
}

export type StreamChunk =
  | ProgressChunk
  | MessageChunk
  | CompletedChunk
  | ErrorChunk;
