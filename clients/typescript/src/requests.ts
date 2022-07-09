import {Metadata} from "./types";
import {EncapsulatedResponse, ResponseType, RpcResponse} from "./response";
import {Client} from "./client";
import {AsyncQueue} from "./util";

export enum RequestType {
    MESSAGE = 0,
    END_OF_STREAM = 1,
    METADATA = 2,
    CANCEL = 3,
}

export type EncapsulatedRequestArray = [number, any, string, number?, Metadata?];

export type RequestArgs = object;

export interface CallOptions {
  metadata?: Metadata;
  sendMetadataFirst?: boolean
}


export const createEncapsulatedRequest = (
    sequence: number, data: any, rpc: string, type: RequestType = RequestType.MESSAGE, metadata?: Metadata
) => {
    const requestArray: EncapsulatedRequestArray = [
      sequence,
      data,
      rpc
    ];

    if (type != RequestType.MESSAGE || metadata !== undefined) {
      requestArray.push(type);
    }

    if (metadata !== undefined) {
      requestArray.push(metadata);
    }
    return requestArray;
}


export class RpcRequest<RequestMessage, ResponseMessage> {

    private client: Client;
    private readonly initialRequest: EncapsulatedRequestArray;
    private receiveQueue = new AsyncQueue<RpcResponse<ResponseMessage>>();
    private requestEnded = false;
    private _leadingMetadata?: Metadata;
    private _trailingMetadata?: Metadata;

    constructor(client: Client, initialRequest: EncapsulatedRequestArray) {
        this.initialRequest = initialRequest;
        this.client = client;
    }

    /** Send message to the server */
    send(message: RequestMessage) {
        if (this.requestEnded) {
            throw new Error('Request has already ended');
        }
        const requestArray = createEncapsulatedRequest(
            this.initialRequest[0],
            message,
            this.initialRequest[2]
        );
        this.client.sendEncapsulatedRequest(requestArray);
    }

    /** Receive data from the server. Blocks until data is received. Returns as Response object */
    async receive() {
        return this.receiveQueue.get();
    }

    close() {
        this.receiveQueue.close();
        this.requestEnded = true;
        this.client.removeStreamReference(this.initialRequest[0]);
    }

    /** Cancel the request by sending Cancel to the server */
    cancel() {
        const requestArray = createEncapsulatedRequest(
            this.initialRequest[0],
            null,
            this.initialRequest[2],
            RequestType.CANCEL
        );
        this.client.sendEncapsulatedRequest(requestArray);
    }

    /** Send EndOfStream and close the streaming request */
    endStream() {
        const requestArray = createEncapsulatedRequest(
            this.initialRequest[0],
            null,
            this.initialRequest[2],
            RequestType.END_OF_STREAM
        );
        this.client.sendEncapsulatedRequest(requestArray);
    }

    /** Returns true if there is data available to receive */
    get hasData() {
        return this.receiveQueue.length > 0;
    }

    /** Return if the stream has ended */
    get ended() {
        return this.requestEnded;
    }

    /** Return the leading metadata if any exists */
    get leadingMetadata() {
        return this._leadingMetadata;
    }

    /** Return the trailing metadata */
    get trailingMetadata() {
        return this._trailingMetadata;
    }

    processMessage(response_message: EncapsulatedResponse<ResponseMessage>) {
        if (response_message.leading_metadata) {
            this._leadingMetadata = response_message.leading_metadata;
        }

        if (response_message.trailing_metadata) {
            this._trailingMetadata = response_message.trailing_metadata;
        }

        if (response_message.type == ResponseType.END_OF_STREAM || response_message.type == ResponseType.ERROR) {
            this.requestEnded = true;
        }

        if (response_message.type == ResponseType.ERROR) {
            this.receiveQueue.push({
                type: ResponseType.ERROR,
                data: response_message.data,
                leadingMetadata: this._leadingMetadata,
                trailingMetadata: this._trailingMetadata,
            });
        } else if (response_message.type == ResponseType.MESSAGE) {
            this.receiveQueue.push({
                type: ResponseType.MESSAGE,
                data: response_message.data,
                leadingMetadata: this._leadingMetadata,
                trailingMetadata: this._trailingMetadata,
            });
        } else if (response_message.type == ResponseType.METADATA) {
            this.receiveQueue.push({
                type: response_message.type,
                leadingMetadata: this._leadingMetadata,
                trailingMetadata: this._trailingMetadata,
            });
        }

        if (this.requestEnded) {
            this.receiveQueue.close();
        }

        return !this.requestEnded;
    }

}
