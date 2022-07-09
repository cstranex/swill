
export type Metadata = {
  [key: string]: any;
}

export interface ReconnectOptions {
    delay?: number;
    retries?: number;
    maxJitter?: number; // Upper bound jitter to add
    minJitter?: number; // Lower bound jitter to add
    backoffFactor?: number;
}

export interface ClientOptions {
    url?: string;
    autoConnect?: boolean;
    connectTimeout?: number;
    reconnect?: ReconnectOptions | false;

    onReconnect?: () => void;
    onDisconnected?: (code: number, reason: string) => void;
    onConnected?: () => void;
    onTimeout?: () => void;

    onError?: () => void;
}
