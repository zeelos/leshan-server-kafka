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

import io.zeelos.leshan.avro.request.AvroRequestKind;
import io.zeelos.leshan.avro.response.*;
import io.zeelos.leshan.server.kafka.utils.AvroSerializer;
import org.eclipse.californium.core.Utils;
import org.eclipse.leshan.ResponseCode;
import org.eclipse.leshan.core.node.LwM2mNode;
import org.eclipse.leshan.core.response.ObserveResponse;
import org.eclipse.leshan.core.response.ReadResponse;

/**
 * Functions for serialize and deserialize a LWM2M observe response in Avro.
 * <p>
 * NOTE: we didn't reuse 'ResponseSerDes' in order to have a distinct avro schema for observe responses.
 */
public class ResponseObserveSerDes {

    public static AvroResponseObserve aSerialize(String serverId, String ticket, String endpoint, ObserveResponse response) {
        AvroResponseObserve.Builder aResponseObserveBuilder = AvroResponseObserve.newBuilder();
        aResponseObserveBuilder.setServerId(serverId);
        aResponseObserveBuilder.setTimestamp(System.currentTimeMillis());
        aResponseObserveBuilder.setTicket(ticket == null ? "unset" : ticket);
        aResponseObserveBuilder.setObservationId(Utils.toHexString(response.getObservation().getId()));
        aResponseObserveBuilder.setEp(endpoint);
        aResponseObserveBuilder.setPath(response.getObservation().getPath().toString());

        AvroResponsePayload.Builder aResponsePayloadBuilder = AvroResponsePayload.newBuilder();
        aResponsePayloadBuilder.setKind(AvroRequestKind.observe);
        aResponsePayloadBuilder.setCode(AvroResponseCode.valueOf(response.getCode().getName()));

        if (response.isFailure()) {
            AvroGenericResponse.Builder aGenericResponseBuilder = AvroGenericResponse.newBuilder();
            aGenericResponseBuilder.setMessage(response.getErrorMessage().equals("") ? response.getCode().getName() : response.getErrorMessage());
            aResponsePayloadBuilder.setBody(aGenericResponseBuilder.build());

            aResponseObserveBuilder.setRep(aResponsePayloadBuilder.build());

            return aResponseObserveBuilder.build();
        }

        AvroReadResponseBody.Builder aReadObserveResponseBuilder = AvroReadResponseBody.newBuilder();
        aReadObserveResponseBuilder.setContent(LwM2mNodeSerDes.aSerialize(response.getObservation().getPath(), ((ReadResponse) response).getContent()));
        aResponsePayloadBuilder.setBody(aReadObserveResponseBuilder.build());

        aResponseObserveBuilder.setRep(aResponsePayloadBuilder.build());

        return aResponseObserveBuilder.build();
    }

    public static byte[] bSerialize(String serverId, String ticket, String endpoint, ObserveResponse response) throws Exception {
        return AvroSerializer.toBytes(aSerialize(serverId, ticket, endpoint, response), AvroResponseObserve.class);
    }

    public static ObserveResponse deserialize(AvroResponseObserve aObj) {
        AvroResponsePayload aResponsePayload = aObj.getRep();

        AvroResponseCode aCode = aResponsePayload.getCode();
        if (aCode == null)
            throw new IllegalStateException("Invalid response, missing code attribute");

        ResponseCode code = ResponseCode.fromName(aCode.name());

        AvroRequestKind kind = aResponsePayload.getKind();

        switch (kind) {
            case observe:
                LwM2mNode observeContent = LwM2mNodeSerDes.deserialize(aResponsePayload.getBody());
                return new ObserveResponse(code, observeContent, null, null, null);
            default:
                throw new IllegalStateException("Invalid request missing kind attribute");
        }
    }
}
