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

import io.zeelos.leshan.avro.registration.*;
import io.zeelos.leshan.server.kafka.utils.AvroSerializer;
import org.eclipse.leshan.Link;
import org.eclipse.leshan.core.request.BindingMode;
import org.eclipse.leshan.core.request.Identity;
import org.eclipse.leshan.server.registration.Registration;
import org.eclipse.leshan.server.registration.RegistrationUpdate;

import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Functions for serialize and deserialize a ClientUpdate in Avro.
 */
public class RegistrationUpdateSerDes {

    public static AvroRegistrationResponse aSerialize(String serverId, RegistrationUpdate u, Registration r) {
        AvroRegistrationResponse.Builder aRegistrationResponseBuilder = AvroRegistrationResponse.newBuilder();
        aRegistrationResponseBuilder.setKind(AvroRegistrationKind.UPDATE);

        AvroRegistrationUpdate.Builder aRegistrationUpdateBuilder = AvroRegistrationUpdate.newBuilder()
                .setRegId(u.getRegistrationId())
                .setServerId(serverId)
                .setEp(r.getEndpoint())
                .setAddress(u.getAddress().getHostAddress())
                .setLt(u.getLifeTimeInSec())
                .setSms(u.getSmsNumber())
                .setPort(u.getPort())
                .setLastUp(r.getLastUpdate().getTime());


        if (u.getBindingMode() != null) {
            aRegistrationUpdateBuilder.setBnd(AvroBnd.valueOf(u.getBindingMode().name()));
        }

        if (u.getObjectLinks() != null) {
            List<AvroLink> aLinks = new ArrayList<>();

            for (Link l : u.getObjectLinks()) {
                AvroLink.Builder aLinkBuilder = AvroLink.newBuilder();

                aLinkBuilder.setUrl(l.getUrl());

                Map<String, String> entries = new HashMap<>();
                for (Map.Entry<String, Object> e : l.getAttributes().entrySet()) {
                    // todo: more research on string/int case
                    //  if (e.getValue() instanceof Integer) {
                    //     at.add(e.getKey(), (int) e.getValue());

                    entries.put(e.getKey(), e.getValue().toString());
                }

                aLinkBuilder.setAt(entries);
                aLinks.add(aLinkBuilder.build());
            }
            aRegistrationUpdateBuilder.setLinks(aLinks);
        }

        if (!u.getAdditionalAttributes().isEmpty()) {
            aRegistrationUpdateBuilder.setAttributes(u.getAdditionalAttributes());
        }

        aRegistrationResponseBuilder.setBody(aRegistrationUpdateBuilder.build());

        return aRegistrationResponseBuilder.build();
    }

    public static byte[] bSerialize(String serverId, RegistrationUpdate u, Registration r) throws Exception {
        return AvroSerializer.toBytes(aSerialize(serverId, u, r), AvroRegistrationResponse.class);
    }

    public static RegistrationUpdate deserialize(AvroRegistrationResponse response) throws UnknownHostException {
        AvroRegistrationUpdate aObj = (AvroRegistrationUpdate) response.getBody();

        // convert AvroLink to Link
        List<AvroLink> aLinks = aObj.getLinks();
        Link[] linkObjs = new Link[aLinks.size()];
        for (int i = 0; i < aLinks.size(); i++) {
            AvroLink aLink = aLinks.get(i);
            Link o = new Link(aLink.getUrl(), aLink.getAt());
            linkObjs[i] = o;
        }

        return new RegistrationUpdate(aObj.getRegId(),
                Identity.unsecure(new InetSocketAddress(aObj.getAddress(), aObj.getPort()).getAddress(), aObj.getPort()),
                aObj.getLt(),
                aObj.getSms(),
                BindingMode.valueOf(aObj.getBnd().name()),
                linkObjs,
                aObj.getAttributes());
    }
}