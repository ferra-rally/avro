package org.apache.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.apache.avro.example.SampleClass;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

@RunWith(value = Parameterized.class)
public class BinaryDecoderArrayTest {
  private List<Integer> array;
  private byte[] data;
private BinaryDecoder decoder;

  public BinaryDecoderArrayTest(List<Integer> array) {
    this.array = array;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {new ArrayList<Integer>() {{add(8); add(3); add(8);}}},
      {new ArrayList<Integer>() {{}}}
    });
  }

  @Before
  public void configuration() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
    encoder.writeArrayStart();
    encoder.setItemCount(array.size());

    for(Integer integer : array) {
      encoder.startItem();
      encoder.writeInt(integer);
    }

    encoder.writeArrayEnd();
    encoder.flush();

    data = stream.toByteArray();
    decoder = DecoderFactory.get().binaryDecoder(data, null);
  }


  @Test
  public void arrayTest() throws IOException {

    for(long i = decoder.readArrayStart(); i != 0; i = decoder.arrayNext()) {
      for (int j = 0; j < i; j++) {
        int out = decoder.readInt();
        int expected = array.get(j);
        Assert.assertEquals(expected, out);
      }
    }
  }

}
