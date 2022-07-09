import {decodeAsync, encode} from '@msgpack/msgpack';
import {
  CallOptions,
  createEncapsulatedRequest,
  EncapsulatedRequestArray,
  RequestArgs,
  RequestType,
  RpcRequest,
} from "./requests";
import {
  EncapsulatedResponseArray,
  makeEncapsulatedResponse,
  ResponseError,
  ResponseType
} from './response';
import {ClientOptions} from "./types";
import {AsyncFlag, wait} from "./util";

export class Client {
  private socket?: WebSocket;
  private connectionFlag?: AsyncFlag;
  private connectionAttempts = 0;
  private currentSequence = 0;
  private streams = new Map<number, RpcRequest<any, any>>();
  private options: ClientOptions = {
    autoConnect: true,
    reconnect: {
      delay: 1000,
      retries: 5,
      backoffFactor: 1,
      minJitter: 1000,
      maxJitter: 5000,
    },
    connectTimeout: 30000,
  };

  connected = false;

  constructor(options?: ClientOptions) {
    if (options) {
      Object.keys(options).forEach((key) => {
        (this.options as any)[key] = (options as any)[key];
      });
    }

    if (this.options.autoConnect && this.options.url) {
      this.connect(this.options.url).catch(() => {});
    }
  }

  async connect(url?: string) {
    if (!url) {
      url = this.options.url;
    }
    if (!url) {
      throw new Error('Url must be specified');
    }
    if (this.socket?.readyState === WebSocket.OPEN) {
      return;
    }
    this.connectionFlag = new AsyncFlag();
    this.socket = new WebSocket(url, 'swill/1');
    this.socket.onopen = this.onConnected;
    this.socket.onmessage = this.onMessage;
    this.socket.onclose = this.onClose;
    if (this.options.connectTimeout) {
      try {
        await this.connectionFlag.wait(this.options.connectTimeout);
      } catch (e) {
        if (this.options.onTimeout) {
          this.options.onTimeout();
        }
        await this.reconnect();
      }
    }
  }

  private async reconnect() {
    this.connectionAttempts += 1;

    if (!this.options.reconnect) {
      return;
    }

    if (this.options.reconnect.retries && this.connectionAttempts > this.options.reconnect.retries) {
      // Failed with maximum retries
      return;
    }

    if (this.options.onReconnect) {
      this.options.onReconnect();
    }
    // Wait for a period
    if (this.options.reconnect.delay) {
      let jitter = this.options.reconnect.minJitter || 0;
      if (this.options.reconnect.maxJitter) {
        jitter += (Math.random() * (jitter - this.options.reconnect.maxJitter));
      }
      const factor = (this.options.reconnect.backoffFactor || 1) * this.connectionAttempts;
      const delay = (this.options.reconnect.delay * factor) + Math.abs(jitter);
      await wait(delay);
    }
    await this.connect();
  }

  private onClose = async (e: CloseEvent) => {
    this.connectionFlag?.set();
    this.connected = false;
    // Invalidate all existing streams
    this.streams.forEach((stream) => {
      stream.close();
    });
    this.streams.clear();
    if (this.options.onDisconnected) {
      this.options.onDisconnected(e.code, e.reason);
    }
    if (e.code === 1006 || e.code == 1015 || !e.wasClean) {
      await this.reconnect();
    }
  }

  private onConnected = () => {
    // Reset the current sequence id
    this.currentSequence = 0;
    this.connectionFlag?.set();
    this.connectionAttempts = 0;
    if (this.options.onConnected) {
      this.options.onConnected();
    }
  }

  private onMessage = async (ev: MessageEvent) => {
    const data = await decodeAsync((ev.data as Blob).stream()) as EncapsulatedResponseArray;
    const message = makeEncapsulatedResponse<any>(data);
    // Look up the sequence id
    const response = this.streams.get(message.seq);
    if (!response) {
      throw new Error("Response to a sequence that does not exist");
    }
    const expectsMore = response.processMessage(message);
    if (!expectsMore) {
      // Invalidate the stream reference
      this.streams.delete(message.seq);
    }
  }

  sendEncapsulatedRequest = (requestArray: EncapsulatedRequestArray) => {
    this.socket?.send(encode(requestArray));
  };

  removeStreamReference = (sequenceId: number) => {
    this.streams.delete(sequenceId);
  }

  /** Call the given method with arguments. Returns the result.
   * Either args should be specified or dataCallback. */
  async call<
    ResponseMessage,
  >(
    methodName: string,
    args: RequestArgs,
    options?: CallOptions
  ): Promise<ResponseMessage | null> {
    const request = this.rpc<ResponseMessage, typeof args>(methodName, args, options);
    const response = await request.receive();
    request.close();
    if (!response) {
      throw new Error('No response received');
    } else if (response?.type === ResponseType.MESSAGE) {
      return response.data;
    } else if (response.type == ResponseType.ERROR) {
      throw new ResponseError(response.data);
    } else {
      return null;
    }
  }

  /** Lower-level calling of RPC-functions. Returns a Request object */
  rpc<ResponseMessage, RequestMessage>(
    methodName: string, args?: RequestMessage, options?: CallOptions
  ): RpcRequest<RequestMessage, ResponseMessage>
  {
    this.currentSequence++;
    const requestSequenceId = this.currentSequence;

    let requestArray;
    if (options?.sendMetadataFirst && !args) {
      requestArray = createEncapsulatedRequest(
          requestSequenceId, undefined, methodName, RequestType.METADATA, options?.metadata || {}
      );
    } else {
      requestArray = createEncapsulatedRequest(
          requestSequenceId, args, methodName, RequestType.MESSAGE, options?.metadata
      );
    }
    const request = new RpcRequest<RequestMessage, ResponseMessage>(this, requestArray);
    this.streams.set(requestSequenceId, request);
    this.sendEncapsulatedRequest(requestArray);
    return request;
  }
}
