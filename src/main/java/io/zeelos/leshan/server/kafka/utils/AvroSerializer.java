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

package io.zeelos.leshan.server.kafka.utils;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.ByteArrayOutputStream;

public class AvroSerializer {

    public static <V> byte[] toBytes(V v, Class<V> cls) throws Exception {
        ByteArrayOutputStream bout = new ByteArrayOutputStream();

        SpecificDatumWriter<V> writer = new SpecificDatumWriter<V>(cls);
        BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);

        writer.write(v, binEncoder);
        binEncoder.flush();

        return bout.toByteArray();
    }

    public static <V> V fromBytes(byte[] bytes, Class<V> cls) throws Exception {
        SpecificDatumReader<V> reader = new SpecificDatumReader<V>(cls);

        BinaryDecoder binDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, binDecoder);
    }
}
