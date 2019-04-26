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

import io.zeelos.leshan.avro.resource.*;
import org.eclipse.leshan.core.model.ResourceModel.Type;
import org.eclipse.leshan.core.node.*;
import org.eclipse.leshan.util.Base64;

import java.util.*;
import java.util.Map.Entry;

/**
 * Functions for serialize and deserialize a LWM2M node in Avro.
 */
public class LwM2mNodeSerDes {

    public static Object aSerialize(LwM2mPath path, LwM2mNode n) {
        if (n instanceof LwM2mObject) {
            return serializeObject(path, (LwM2mObject) n);
        } else if (n instanceof LwM2mObjectInstance) {
            return serializeObjectInstance(path, (LwM2mObjectInstance) n);
        } else if (n instanceof LwM2mResource) {
            return serializeResource(path, (LwM2mResource) n);
        }

        return null;
    }

    private static AvroObjectResource serializeObject(LwM2mPath path, LwM2mObject obj) {
        AvroObjectResource.Builder aObjBuilder = AvroObjectResource.newBuilder();
        aObjBuilder.setId(path.getObjectId());
        aObjBuilder.setKind(AvroResourceKind.OBJECT);

        List<AvroInstanceResource> aInstances = new ArrayList<>();
        for (LwM2mObjectInstance instance : obj.getInstances().values()) {
            aInstances.add(serializeObjectInstance(new LwM2mPath(path.getObjectId(), instance.getId()), instance));
        }

        aObjBuilder.setInstances(aInstances);

        return aObjBuilder.build();
    }

    private static LwM2mNode deserializeObject(AvroObjectResource aObj) {
        Collection<LwM2mObjectInstance> instances = new ArrayList<>();
        List<AvroInstanceResource> aResources = aObj.getInstances();

        for (AvroInstanceResource aResource : aResources) {
            LwM2mObjectInstance instance = (LwM2mObjectInstance) deserializeObjectInstance(aResource);
            instances.add(instance);
        }

        return new LwM2mObject(aObj.getId(), instances);
    }

    private static AvroInstanceResource serializeObjectInstance(LwM2mPath path, LwM2mObjectInstance instance) {
        AvroInstanceResource.Builder aInstBuilder = AvroInstanceResource.newBuilder();
        aInstBuilder.setId(instance.getId());
        aInstBuilder.setKind(AvroResourceKind.INSTANCE);

        List<AvroResource> aResources = new ArrayList<>();
        List<AvroMultipleResource> aMultipleResources = null; // lazy load

        for (LwM2mResource resource : instance.getResources().values()) {
            if (resource.isMultiInstances()) {
                if (aMultipleResources == null)
                    aMultipleResources = new ArrayList<>();
                aMultipleResources.add(serializeMultipleResource(
                        new LwM2mPath(path.getObjectId(), path.getObjectInstanceId(), resource.getId()), resource));
            } else {
                aResources.add(serializeSingleResource(
                        new LwM2mPath(path.getObjectId(), path.getObjectInstanceId(), resource.getId()), resource));
            }
        }

        aInstBuilder.setResources(aResources);
        if (aMultipleResources != null)
            aInstBuilder.setMultipleResources(aMultipleResources);

        return aInstBuilder.build();
    }

    private static LwM2mObjectInstance deserializeObjectInstance(AvroInstanceResource aObj) {
        Collection<LwM2mResource> resources = new ArrayList<>();

        List<AvroResource> aResources = aObj.getResources();
        for (AvroResource aResource : aResources) {
            resources.add(deserializeSingleResource(aResource));
        }

        List<AvroMultipleResource> aMultipleResources = aObj.getMultipleResources();
        if (aMultipleResources != null) {
            for (AvroMultipleResource aMultipleResource : aMultipleResources) {
                resources.add(deserializeMultipleResource(aMultipleResource));
            }
        }

        return new LwM2mObjectInstance(aObj.getId(), resources);
    }

    private static AvroMultipleResource serializeMultipleResource(LwM2mPath path, LwM2mResource resource) {
        AvroMultipleResource.Builder aMultipleResourceBuilder = AvroMultipleResource.newBuilder();
        aMultipleResourceBuilder.setId(resource.getId());
        aMultipleResourceBuilder.setPath(path.toString());
        aMultipleResourceBuilder.setKind(AvroResourceKind.MULTIPLE_RESOURCE);
        aMultipleResourceBuilder.setType(AvroType.valueOf(resource.getType().name()));

        Map<String, AvroResource> aMultipleResources = new HashMap<>();

        for (Entry<Integer, ?> entry : resource.getValues().entrySet()) {
            AvroResource.Builder aSingleBuilder = AvroResource.newBuilder();
            aSingleBuilder.setId(entry.getKey());
            aSingleBuilder.setPath(
                    new LwM2mPath(path.getObjectId(), path.getObjectInstanceId(), path.getResourceId(), entry.getKey())
                            .toString());
            aSingleBuilder.setKind(AvroResourceKind.SINGLE_RESOURCE);
            aSingleBuilder.setType(AvroType.valueOf(resource.getType().name()));
            aSingleBuilder.setValue(aValue(resource.getType(), entry.getValue()));

            aMultipleResources.put(entry.getKey().toString(), aSingleBuilder.build());
        }

        aMultipleResourceBuilder.setResources(aMultipleResources);

        return aMultipleResourceBuilder.build();
    }

    private static LwM2mMultipleResource deserializeMultipleResource(AvroMultipleResource aResource) {
        Map<Integer, Object> values = new HashMap<>();

        Map<String, AvroResource> aResources = aResource.getResources();
        for (Entry<String, AvroResource> aEntry : aResources.entrySet()) {
            values.put(Integer.valueOf(aEntry.getKey()), aEntry.getValue());
        }

        return LwM2mMultipleResource.newResource(aResource.getId(), values, Type.valueOf(aResource.getType().name()));
    }

    private static AvroResource serializeSingleResource(LwM2mPath path, LwM2mResource resource) {
        AvroResource.Builder aSingleResourceBuilder = AvroResource.newBuilder();

        aSingleResourceBuilder.setId(resource.getId());
        aSingleResourceBuilder.setPath(path.toString());
        aSingleResourceBuilder.setKind(AvroResourceKind.SINGLE_RESOURCE);
        aSingleResourceBuilder.setType(AvroType.valueOf(resource.getType().name()));
        aSingleResourceBuilder.setValue(aValue(resource.getType(), resource.getValue()));

        return aSingleResourceBuilder.build();
    }

    private static LwM2mSingleResource deserializeSingleResource(AvroResource aResource) {
        return LwM2mSingleResource.newResource(aResource.getId(), aResource.getValue(),
                Type.valueOf(aResource.getType().name()));
    }

    private static Object serializeResource(LwM2mPath path, LwM2mResource resource) {
        if (resource.isMultiInstances()) {
            return serializeMultipleResource(path, resource);
        } else {
            return serializeSingleResource(path, resource);
        }
    }

    private static Object aValue(Type type, Object value) {
        if (type == Type.OPAQUE) {
            return Base64.encodeBase64String((byte[]) value);
        } else if (type == Type.TIME) {
            return ((Date) value).getTime();
        } else {
            return value;
        }
    }

    public static LwM2mNode deserialize(Object aObj) {
        if (aObj instanceof AvroObjectResource) {
            return deserializeObject((AvroObjectResource) aObj);
        } else if (aObj instanceof AvroInstanceResource) {
            return deserializeObjectInstance((AvroInstanceResource) aObj);
        } else if (aObj instanceof AvroResource) {
            return deserializeSingleResource((AvroResource) aObj);
        } else if (aObj instanceof AvroMultipleResource) {
            return deserializeMultipleResource((AvroMultipleResource) aObj);
        }

        throw new IllegalStateException("Invalid Avro LwM2mNode unable to deserialize!");
    }
}
