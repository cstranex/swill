import {Metadata} from "./types";

export enum ResponseType {
    MESSAGE = 0,
    END_OF_STREAM = 1,
    METADATA = 2,
    ERROR = 3,
}

export interface ErrorMessage {
  code: number;
  message: string;
  data?: object;
}

export type EncapsulatedResponseArray = [
  number,
  any,
  ResponseType?,
  Metadata?,
  Metadata?
];

export function makeEncapsulatedResponse<T>(data: EncapsulatedResponseArray) {
  return {
    seq: data[0],
    data: data[1],
    type: data[2] || ResponseType.MESSAGE,
    leading_metadata: data[3],
    trailing_metadata: data[4],
  } as EncapsulatedResponse<T>;
}

interface BaseEncapsulatedResponse {
  seq: number;
  leading_metadata?: Metadata;
  trailing_metadata?: Metadata;
}

interface EncapsulatedMessageResponse<T> extends BaseEncapsulatedResponse {
  type: ResponseType.MESSAGE;
  data: T;
}

interface EncapsulatedNullResponse extends BaseEncapsulatedResponse {
  type: ResponseType.END_OF_STREAM | ResponseType.METADATA;
}

interface EncapsulatedErrorResponse extends BaseEncapsulatedResponse {
  type: ResponseType.ERROR;
  data: ErrorMessage;
}

export type EncapsulatedResponse<T> = EncapsulatedMessageResponse<T> | EncapsulatedErrorResponse | EncapsulatedNullResponse;

interface BaseRpcResponse {
  leadingMetadata?: Metadata;
  trailingMetadata?: Metadata;
}

interface RpcMessageResponse<T> extends BaseRpcResponse {
  type: ResponseType.MESSAGE;
  data: T;
}

interface RpcErrorResponse extends BaseRpcResponse {
  type: ResponseType.ERROR;
  data: ErrorMessage;
}

interface RpcNullMessageResponse extends BaseRpcResponse {
  type: ResponseType.END_OF_STREAM | ResponseType.METADATA;
}

export type RpcResponse<T> = RpcMessageResponse<T> | RpcErrorResponse | RpcNullMessageResponse;

export class ResponseError extends Error {
  responseMessage: ErrorMessage;

  constructor(message: ErrorMessage) {
    super(message.message);
    this.responseMessage = message;
  }

  get code() {
    return this.responseMessage.code;
  }

  get message() {
    return this.responseMessage.message;
  }

  get data() {
    return this.responseMessage.data;
  }

}
