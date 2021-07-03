package org.apache.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

@RunWith(value = Parameterized.class)
public class BinaryDecoderMapTest {
  private byte[] data;
  private HashMap<String, Integer> map;

  public BinaryDecoderMapTest(HashMap<String, Integer> map) {
    this.map = map;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {new HashMap<String, Integer>() {{put("test", 3);}}}
    });
  }

  @Before
  public void config() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

    encoder.writeMapStart();
    encoder.setItemCount(map.size());

    for(String key : map.keySet()) {
      encoder.startItem();
      encoder.writeString(key);
      encoder.writeInt(map.get(key));
    }

    encoder.writeMapEnd();
    encoder.flush();

    data = stream.toByteArray();
  }

  @Test
  public void readMapTest() throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

    Map m = new HashMap(map);
    for (long i = decoder.readMapStart(); i != 0; i = decoder.mapNext()) {
      for (long j = 0; j < i; j++) {
        String key = decoder.readString();
        int value = decoder.readInt();

        if(m.containsKey(key)) {
          Assert.assertEquals(m.get(key), value);
          m.remove(key);
        }
      }
    }

    Assert.assertTrue(m.isEmpty());
  }
}

