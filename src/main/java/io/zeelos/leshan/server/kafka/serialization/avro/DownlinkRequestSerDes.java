/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zeelos.leshan.server.kafka.serialization.avro;

import io.zeelos.leshan.avro.request.*;
import io.zeelos.leshan.avro.resource.AvroInstanceResource;
import io.zeelos.leshan.server.kafka.utils.AvroSerializer;
import org.eclipse.leshan.core.attributes.AttributeSet;
import org.eclipse.leshan.core.node.LwM2mNode;
import org.eclipse.leshan.core.node.LwM2mObjectInstance;
import org.eclipse.leshan.core.request.*;
import org.eclipse.leshan.core.request.WriteRequest.Mode;

/**
 * Functions for serialize and deserialize a LWM2M Downlink request in Avro.
 */
public class DownlinkRequestSerDes {

    public static AvroRequest aSerialize(String ticket, String endpoint, DownlinkRequest<?> r) {
        AvroRequest.Builder aRequestBuilder = AvroRequest.newBuilder();

        aRequestBuilder.setTicket(ticket);
        aRequestBuilder.setEp(endpoint);

        final AvroRequestPayload.Builder aRequestPayloadBuilder = AvroRequestPayload.newBuilder();

        aRequestPayloadBuilder.setPath(r.getPath().toString());

        r.accept(new DownLinkRequestVisitorAdapter() {
            @Override
            public void visit(ObserveRequest request) {
                aRequestPayloadBuilder.setKind(AvroRequestKind.observe);

                if (request.getContentFormat() != null) {
                    aRequestPayloadBuilder
                            .setContentFormat(AvroContentFormat.valueOf(request.getContentFormat().getName()));
                }
            }

            @Override
            public void visit(DeleteRequest request) {
                aRequestPayloadBuilder.setKind(AvroRequestKind.delete);
            }

            @Override
            public void visit(DiscoverRequest request) {
                aRequestPayloadBuilder.setKind(AvroRequestKind.discover);
            }

            @Override
            public void visit(CreateRequest request) {
                aRequestPayloadBuilder.setKind(AvroRequestKind.create);
                aRequestPayloadBuilder
                        .setContentFormat(AvroContentFormat.valueOf(request.getContentFormat().getName()));

                AvroCreateRequest.Builder aCreateRequestBuilder = AvroCreateRequest.newBuilder();
                aCreateRequestBuilder.setInstance((AvroInstanceResource) LwM2mNodeSerDes.aSerialize(request.getPath(),
                        new LwM2mObjectInstance(request.getInstanceId(), request.getResources())));

                aRequestPayloadBuilder.setBody(aCreateRequestBuilder.build());
            }

            @Override
            public void visit(ExecuteRequest request) {
                aRequestPayloadBuilder.setKind(AvroRequestKind.execute);

                AvroExecuteRequest.Builder aExecuteRequestBuilder = AvroExecuteRequest.newBuilder();
                aExecuteRequestBuilder.setParameters(request.getParameters());

                aRequestPayloadBuilder.setBody(aExecuteRequestBuilder.build());
            }

            @Override
            public void visit(WriteAttributesRequest request) {
                aRequestPayloadBuilder.setKind(AvroRequestKind.writeAttributes);

                AvroWriteAttributesRequest.Builder aWriteAttributesRequestBuilder = AvroWriteAttributesRequest
                        .newBuilder();
                aWriteAttributesRequestBuilder.setObserveSpec(request.getAttributes().toString());

                aRequestPayloadBuilder.setBody(aWriteAttributesRequestBuilder.build());
            }

            @Override
            public void visit(WriteRequest request) {
                aRequestPayloadBuilder.setKind(AvroRequestKind.write);
                aRequestPayloadBuilder
                        .setContentFormat(AvroContentFormat.valueOf(request.getContentFormat().getName()));

                AvroWriteRequest.Builder aWriteRequestBuilder = AvroWriteRequest.newBuilder();
                aWriteRequestBuilder.setMode(
                        request.isPartialUpdateRequest() ? AvroWriteRequestMode.UPDATE : AvroWriteRequestMode.REPLACE);
                aWriteRequestBuilder.setNode(LwM2mNodeSerDes.aSerialize(request.getPath(), request.getNode()));

                aRequestPayloadBuilder.setBody(aWriteRequestBuilder.build());
            }

            @Override
            public void visit(ReadRequest request) {
                aRequestPayloadBuilder.setKind(AvroRequestKind.read);
                aRequestPayloadBuilder
                        .setContentFormat(AvroContentFormat.valueOf(request.getContentFormat().getName()));
            }
        });

        aRequestBuilder.setPayload(aRequestPayloadBuilder.build());

        return aRequestBuilder.build();
    }

    public static byte[] bSerialize(String ticket, String endpoint, DownlinkRequest<?> r) throws Exception {
        return AvroSerializer.toBytes(aSerialize(ticket, endpoint, r), AvroRequest.class);
    }

    public static DownlinkRequest<?> deserialize(AvroRequest avroRequest) {
        AvroRequestPayload aRequestPayload = avroRequest.getPayload();

        String path = aRequestPayload.getPath();
        AvroContentFormat aContentFormat = aRequestPayload.getContentFormat();
        AvroRequestKind avroKind = aRequestPayload.getKind();

        switch (avroKind) {
        case observe:
            return new ObserveRequest(ContentFormat.fromName(aContentFormat.name()), path);
        case delete:
            return new DeleteRequest(path);
        case discover:
            return new DiscoverRequest(path);
        case create:
            AvroCreateRequest aCreateRequest = (AvroCreateRequest) aRequestPayload.getBody();
            LwM2mObjectInstance objInstance = (LwM2mObjectInstance) LwM2mNodeSerDes
                    .deserialize(aCreateRequest.getInstance());
            return new CreateRequest(ContentFormat.fromName(aContentFormat.name()), path, objInstance);
        case execute:
            AvroExecuteRequest aExecuteRequest = (AvroExecuteRequest) aRequestPayload.getBody();
            String parameters = aExecuteRequest.getParameters();
            return new ExecuteRequest(path, parameters);
        case writeAttributes:
            AvroWriteAttributesRequest aWriteAttributesRequest = (AvroWriteAttributesRequest) aRequestPayload.getBody();
            String observeSpec = aWriteAttributesRequest.getObserveSpec();
            return new WriteAttributesRequest(path, AttributeSet.parse(observeSpec));
        case write:
            AvroWriteRequest aWriteRequest = (AvroWriteRequest) aRequestPayload.getBody();
            AvroWriteRequestMode aWriteRequestMode = aWriteRequest.getMode();

            Mode mode = Mode.valueOf(aWriteRequestMode.name());

            LwM2mNode node = LwM2mNodeSerDes.deserialize(aWriteRequest.getNode());
            return new WriteRequest(mode, ContentFormat.fromName(aContentFormat.name()), path, node);
        case read:
            return new ReadRequest(ContentFormat.fromName(aContentFormat.name()), path);
        default:
            throw new IllegalStateException("Invalid request missing kind attribute");
        }
    }
}
