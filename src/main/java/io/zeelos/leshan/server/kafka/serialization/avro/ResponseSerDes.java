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

import java.util.Arrays;

import org.eclipse.leshan.Link;
import org.eclipse.leshan.ResponseCode;
import org.eclipse.leshan.core.node.LwM2mNode;
import org.eclipse.leshan.core.node.LwM2mPath;
import org.eclipse.leshan.core.response.CreateResponse;
import org.eclipse.leshan.core.response.DeleteResponse;
import org.eclipse.leshan.core.response.DiscoverResponse;
import org.eclipse.leshan.core.response.ExecuteResponse;
import org.eclipse.leshan.core.response.LwM2mResponse;
import org.eclipse.leshan.core.response.ReadResponse;
import org.eclipse.leshan.core.response.WriteAttributesResponse;
import org.eclipse.leshan.core.response.WriteResponse;

import io.zeelos.leshan.avro.request.AvroRequestKind;
import io.zeelos.leshan.avro.response.AvroCreateResponse;
import io.zeelos.leshan.avro.response.AvroDiscoverResponse;
import io.zeelos.leshan.avro.response.AvroGenericResponse;
import io.zeelos.leshan.avro.response.AvroReadResponseBody;
import io.zeelos.leshan.avro.response.AvroResponse;
import io.zeelos.leshan.avro.response.AvroResponseCode;
import io.zeelos.leshan.avro.response.AvroResponsePayload;
import io.zeelos.leshan.server.kafka.utils.AvroSerializer;

/**
 * Functions for serialize and deserialize a LWM2M response in Avro.
 */
public class ResponseSerDes {

    public static AvroResponse aSerialize(String serverId, String ticket, String endpoint, LwM2mPath path,
            AvroRequestKind kind, LwM2mResponse response) {
        AvroResponse.Builder aResponseBuilder = AvroResponse.newBuilder();
        aResponseBuilder.setServerId(serverId);
        aResponseBuilder.setTimestamp(System.currentTimeMillis());
        aResponseBuilder.setTicket(ticket == null ? "unset" : ticket);
        aResponseBuilder.setEp(endpoint);
        aResponseBuilder.setPath(path.toString());

        AvroResponsePayload.Builder aResponsePayloadBuilder = AvroResponsePayload.newBuilder();
        aResponsePayloadBuilder.setKind(kind);
        aResponsePayloadBuilder.setCode(AvroResponseCode.valueOf(response.getCode().getName()));

        if (response.isFailure()) {
            AvroGenericResponse.Builder aGenericResponseBuilder = AvroGenericResponse.newBuilder();
            aGenericResponseBuilder.setMessage(
                    response.getErrorMessage().equals("") ? response.getCode().getName() : response.getErrorMessage());
            aResponsePayloadBuilder.setBody(aGenericResponseBuilder.build());

            aResponseBuilder.setRep(aResponsePayloadBuilder.build());

            return aResponseBuilder.build();
        }

        if (response instanceof ReadResponse) {
            AvroReadResponseBody.Builder robuilder = AvroReadResponseBody.newBuilder();
            robuilder.setContent(LwM2mNodeSerDes.aSerialize(path, ((ReadResponse) response).getContent()));
            aResponsePayloadBuilder.setBody(robuilder.build());
        } else if (response instanceof DiscoverResponse) {
            AvroDiscoverResponse.Builder aDiscoverResponseBuilder = AvroDiscoverResponse.newBuilder();
            aDiscoverResponseBuilder.setObjectLinks(Arrays.toString(((DiscoverResponse) response).getObjectLinks()));
            aResponsePayloadBuilder.setBody(aDiscoverResponseBuilder.build());
        } else if (response instanceof DeleteResponse) {
            AvroGenericResponse.Builder aGenericResponseBuilder = AvroGenericResponse.newBuilder();
            aGenericResponseBuilder.setMessage(response.getCode().getName());
            aResponsePayloadBuilder.setBody(aGenericResponseBuilder.build());
        } else if (response instanceof ExecuteResponse) {
            AvroGenericResponse.Builder aGenericResponseBuilder = AvroGenericResponse.newBuilder();
            aGenericResponseBuilder.setMessage(response.getCode().getName());
            aResponsePayloadBuilder.setBody(aGenericResponseBuilder.build());
        } else if (response instanceof WriteResponse) {
            AvroGenericResponse.Builder aGenericResponseBuilder = AvroGenericResponse.newBuilder();
            aGenericResponseBuilder.setMessage(response.getCode().getName());
            aResponsePayloadBuilder.setBody(aGenericResponseBuilder.build());
        } else if (response instanceof WriteAttributesResponse) {
            AvroGenericResponse.Builder aGenericResponseBuilder = AvroGenericResponse.newBuilder();
            aGenericResponseBuilder.setMessage(response.getCode().getName());
            aResponsePayloadBuilder.setBody(aGenericResponseBuilder.build());
        } else if (response instanceof CreateResponse) {
            AvroCreateResponse.Builder aCreateResponseBuilder = AvroCreateResponse.newBuilder();
            aCreateResponseBuilder.setLocation(((CreateResponse) response).getLocation());
            aResponsePayloadBuilder.setBody(aCreateResponseBuilder.build());
        }

        aResponseBuilder.setRep(aResponsePayloadBuilder.build());

        return aResponseBuilder.build();
    }

    public static byte[] bSerialize(String serverId, String ticket, String endpoint, LwM2mPath path,
            AvroRequestKind kind, LwM2mResponse response) throws Exception {
        return AvroSerializer.toBytes(aSerialize(serverId, ticket, endpoint, path, kind, response), AvroResponse.class);
    }

    public static LwM2mResponse deserialize(AvroResponse aObj) {
        AvroResponsePayload aResponsePayload = aObj.getRep();

        AvroResponseCode aCode = aResponsePayload.getCode();
        if (aCode == null)
            throw new IllegalStateException("Invalid response, missing code attribute");

        ResponseCode code = ResponseCode.fromName(aCode.name());

        AvroRequestKind kind = aResponsePayload.getKind();

        switch (kind) {
        case read:
            LwM2mNode readContent = LwM2mNodeSerDes.deserialize(aResponsePayload.getBody());
            return new ReadResponse(code, readContent, null);
        case discover:
            String objectLinks = ((AvroDiscoverResponse) aResponsePayload.getBody()).getObjectLinks();
            return new DiscoverResponse(code, Link.parse(objectLinks.getBytes()), null);
        case create:
            String location = ((AvroCreateResponse) aResponsePayload.getBody()).getLocation();
            return new CreateResponse(code, location, null);
        case delete:
            return new DeleteResponse(code, null);
        case execute:
            return new ExecuteResponse(code, null);
        case writeAttributes:
            return new WriteAttributesResponse(code, null);
        case write:
            return new WriteResponse(code, null);
        default:
            throw new IllegalStateException("Invalid request missing kind attribute");
        }
    }
}
