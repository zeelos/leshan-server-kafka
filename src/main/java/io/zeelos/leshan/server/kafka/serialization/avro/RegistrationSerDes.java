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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.leshan.Link;
import org.eclipse.leshan.core.request.BindingMode;
import org.eclipse.leshan.core.request.Identity;
import org.eclipse.leshan.server.registration.Registration;

import io.zeelos.leshan.avro.registration.AvroBnd;
import io.zeelos.leshan.avro.registration.AvroLink;
import io.zeelos.leshan.avro.registration.AvroRegistrationKind;
import io.zeelos.leshan.avro.registration.AvroRegistrationNew;
import io.zeelos.leshan.avro.registration.AvroRegistrationResponse;
import io.zeelos.leshan.server.kafka.utils.AvroSerializer;

/**
 * Functions for serialize and deserialize a Client in Avro.
 */
public class RegistrationSerDes {

    public static AvroRegistrationResponse aSerialize(String serverId, Registration r) {
        AvroRegistrationResponse.Builder aRegistrationResponseBuilder = AvroRegistrationResponse.newBuilder();
        aRegistrationResponseBuilder.setKind(AvroRegistrationKind.NEW);

        AvroRegistrationNew.Builder aRegistrationNewBuilder = AvroRegistrationNew.newBuilder().setRegId(r.getId())
                .setServerId(serverId).setEp(r.getEndpoint()).setRegDate(r.getRegistrationDate().getTime())
                .setAddress(r.getAddress().getHostAddress()).setPort(r.getPort())
                .setRegAddr(r.getRegistrationEndpointAddress().getHostString())
                .setRegPort(r.getRegistrationEndpointAddress().getPort()).setLt(r.getLifeTimeInSec())
                .setSms(r.getSmsNumber()).setVer(r.getLwM2mVersion())
                .setBnd(AvroBnd.valueOf(r.getBindingMode().name()));

        List<AvroLink> links = new ArrayList<>();
        for (Link l : r.getObjectLinks()) {
            AvroLink.Builder aLinkBuilder = AvroLink.newBuilder();

            aLinkBuilder.setUrl(l.getUrl());

            Map<String, String> entries = new HashMap<>();
            for (Map.Entry<String, Object> e : l.getAttributes().entrySet()) {
                entries.put(e.getKey(), e.getValue().toString());
            }

            aLinkBuilder.setAt(entries);
            links.add(aLinkBuilder.build());
        }
        aRegistrationNewBuilder.setLinks(links);

        aRegistrationNewBuilder.setAttributes(r.getAdditionalRegistrationAttributes());
        aRegistrationNewBuilder.setRoot(r.getRootPath());
        aRegistrationNewBuilder.setLastUp(r.getLastUpdate().getTime());

        aRegistrationResponseBuilder.setBody(aRegistrationNewBuilder.build());

        return aRegistrationResponseBuilder.build();
    }

    public static byte[] bSerialize(String serverId, Registration r) throws Exception {
        return AvroSerializer.toBytes(aSerialize(serverId, r), AvroRegistrationResponse.class);
    }

    public static Registration deserialize(AvroRegistrationResponse response) {
        AvroRegistrationNew aObj = (AvroRegistrationNew) response.getBody();

        Registration.Builder aRegistrationBuilder = new Registration.Builder(aObj.getRegId(), aObj.getEp(), Identity
                .unsecure(new InetSocketAddress(aObj.getAddress(), aObj.getPort()).getAddress(), aObj.getPort()),
                new InetSocketAddress(aObj.getRegAddr(), aObj.getRegPort()));

        aRegistrationBuilder.bindingMode(BindingMode.valueOf(aObj.getBnd().name()));
        aRegistrationBuilder.lastUpdate(new Date(aObj.getLastUp()));
        aRegistrationBuilder.lifeTimeInSec(aObj.getLt());
        aRegistrationBuilder.lwM2mVersion(aObj.getVer());
        aRegistrationBuilder.registrationDate(new Date(aObj.getRegDate()));
        aRegistrationBuilder.smsNumber(aObj.getSms());

        List<AvroLink> aLinks = aObj.getLinks();
        Link[] linkObjs = new Link[aLinks.size()];
        for (int i = 0; i < aLinks.size(); i++) {
            AvroLink aLink = aLinks.get(i);
            Link o = new Link(aLink.getUrl(), aLink.getAt());
            linkObjs[i] = o;
        }

        aRegistrationBuilder.objectLinks(linkObjs);
        aRegistrationBuilder.additionalRegistrationAttributes(aObj.getAttributes());

        return aRegistrationBuilder.build();
    }
}