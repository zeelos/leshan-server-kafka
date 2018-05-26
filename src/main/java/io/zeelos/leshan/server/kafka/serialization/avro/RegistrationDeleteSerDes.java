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

import io.zeelos.leshan.avro.registration.AvroRegistrationDelete;
import io.zeelos.leshan.avro.registration.AvroRegistrationKind;
import io.zeelos.leshan.avro.registration.AvroRegistrationResponse;
import io.zeelos.leshan.server.kafka.utils.AvroSerializer;
import org.eclipse.leshan.server.registration.Registration;

/**
 * Functions for serialize and deserialize a Client in Avro.
 */
public class RegistrationDeleteSerDes {

    public static AvroRegistrationResponse aSerialize(String serverId, boolean expired, Registration r) {
        AvroRegistrationResponse.Builder aRegistrationResponseBuilder = AvroRegistrationResponse.newBuilder();
        aRegistrationResponseBuilder.setKind(AvroRegistrationKind.DELETE);

        AvroRegistrationDelete.Builder aRegistrationDeleteBuilder = AvroRegistrationDelete.newBuilder()
                .setServerId(serverId)
                .setEp(r.getEndpoint())
                .setExpired(expired)
                .setLastUp(r.getLastUpdate().getTime());

        aRegistrationResponseBuilder.setBody(aRegistrationDeleteBuilder.build());

        return aRegistrationResponseBuilder.build();
    }

    public static byte[] bSerialize(String serverId, boolean expired, Registration r) throws Exception {
        return AvroSerializer.toBytes(aSerialize(serverId, expired, r), AvroRegistrationResponse.class);
    }

    public static Registration deserialize(AvroRegistrationResponse response) {
        throw new IllegalStateException("deserialize on an AvroRegistrationDelete to a Registration is not applicable currently.");
    }
}