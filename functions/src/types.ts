export interface StreamRequest {
  sources: Array<{
    name: string;
    records: unknown[];
  }>;
}



