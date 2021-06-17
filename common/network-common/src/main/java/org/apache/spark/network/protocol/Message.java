/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.protocol;

import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * An on-the-wire transmittable message.
 * Message是消息的抽象接口，消息实现类都直接或间接的实现了RequestMessage或ResponseMessage接口，其中RequestMessage的具体实现有四种，分别是：
 * <p>
 * ChunkFetchRequest：请求获取流的单个块的序列。ChunkFetch消息用于抽象所有spark中涉及到数据拉取操作时需要传输的消息
 * RpcRequest：此消息类型由远程的RPC服务端进行处理，是一种需要服务端向客户端回复的RPC请求信息类型。
 * OneWayMessage：此消息也需要由远程的RPC服务端进行处理，与RpcRequest不同的是不需要服务端向客户端回复。
 * StreamRequest：此消息表示向远程的服务发起请求，以获取流式数据。Stream消息主要用于driver到executor传输jar、file文件等。
 * <p>
 * 由于OneWayMessage 不需要响应，所以ResponseMessage的对于成功或失败状态的实现各有三种，分别是：
 * <p>
 * ChunkFetchSuccess：处理ChunkFetchRequest成功后返回的消息；
 * ChunkFetchFailure：处理ChunkFetchRequest失败后返回的消息；
 * RpcResponse：处理RpcRequest成功后返回的消息；
 * RpcFailure：处理RpcRequest失败后返回的消息；
 * StreamResponse：处理StreamRequest成功后返回的消息；
 * StreamFailure：处理StreamRequest失败后返回的消息；
 */
public interface Message extends Encodable {
    /**
     * Used to identify this request type.
     */
    Type type();

    /**
     * An optional body for the message.
     */
    ManagedBuffer body();

    /**
     * Whether to include the body of the message in the same frame as the message.
     */
    boolean isBodyInFrame();

    /**
     * Preceding every serialized Message is its type, which allows us to deserialize it.
     */
    enum Type implements Encodable {
        ChunkFetchRequest(0), ChunkFetchSuccess(1), ChunkFetchFailure(2),
        RpcRequest(3), RpcResponse(4), RpcFailure(5),
        StreamRequest(6), StreamResponse(7), StreamFailure(8),
        OneWayMessage(9), UploadStream(10), User(-1);

        private final byte id;

        Type(int id) {
            assert id < 128 : "Cannot have more than 128 message types";
            this.id = (byte) id;
        }

        public byte id() {
            return id;
        }

        @Override
        public int encodedLength() {
            return 1;
        }

        @Override
        public void encode(ByteBuf buf) {
            buf.writeByte(id);
        }

        public static Type decode(ByteBuf buf) {
            byte id = buf.readByte();
            switch (id) {
                case 0:
                    return ChunkFetchRequest;
                case 1:
                    return ChunkFetchSuccess;
                case 2:
                    return ChunkFetchFailure;
                case 3:
                    return RpcRequest;
                case 4:
                    return RpcResponse;
                case 5:
                    return RpcFailure;
                case 6:
                    return StreamRequest;
                case 7:
                    return StreamResponse;
                case 8:
                    return StreamFailure;
                case 9:
                    return OneWayMessage;
                case 10:
                    return UploadStream;
                case -1:
                    throw new IllegalArgumentException("User type messages cannot be decoded.");
                default:
                    throw new IllegalArgumentException("Unknown message type: " + id);
            }
        }
    }
}
