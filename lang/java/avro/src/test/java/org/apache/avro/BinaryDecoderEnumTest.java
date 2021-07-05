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
public class BinaryDecoderEnumTest {
  private List<Integer> enumValues;
  private byte[] data;

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {new ArrayList<Integer>() {{add(2); add(3);}}}
    });
  }

  public BinaryDecoderEnumTest(List<Integer> enumValues) {
    this.enumValues = enumValues;
  }

  @Before
  public void config() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);

    for(Integer integer : enumValues) {
      encoder.writeEnum(integer);
    }

    encoder.flush();

    data = stream.toByteArray();
  }

  @Test
  public void readEnumTest() throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

    for(int integer : enumValues) {
      int out = decoder.readEnum();

      Assert.assertEquals(integer, out);
    }
  }
}
