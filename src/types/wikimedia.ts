export interface WikimediaRecentChange {
  id: number;
  type: string;
  bot: boolean;
  server_name?: string;
  wiki?: string;
  namespace?: number;
  title?: string;
  user?: string;
  comment?: string;
  timestamp?: number;
  minor?: boolean;
  patrolled?: boolean;
  length?: {
    old?: number;
    new?: number;
  };
}

export interface AnalyticsSnapshot {
  processedMessages: number;
  botEvents: number;
  nonBotEvents: number;
  topWikis: Array<{ key: string; count: number }>;
  topEventTypes: Array<{ key: string; count: number }>;
}