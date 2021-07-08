package org.apache.avro;

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
public class BinaryDecoderTest {
  private Schema schema;
  private Encoder encoder;
  private BinaryDecoder decoder;
  private List<SampleClass> sampleClassList;
  //private boolean aBoolean;
  private byte[] data;
  private byte[] editedData;
  private DatumReader<SampleClass> reader;
  private String maxBytesLenght;

  public BinaryDecoderTest(List<SampleClass> sampleClassList, String maxBytesLenght) {
    this.sampleClassList = sampleClassList;
    this.maxBytesLenght = maxBytesLenght;
  }

  /*
    string
    null
    int
    float
    double
    bytes
    boolean
    long
   */
  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {new ArrayList<SampleClass>() {{
        add(new SampleClass("string", null, 1, (float) 1.0, 1.0, null, true, (long) 1));
        add(new SampleClass("", null, 100, (float) 0, 1.0, null, false, (long) 1));
      }},  "4"},
      {new ArrayList<SampleClass>() {{
        add(new SampleClass("string", null, 1, (float) 1.0, 1.0, ByteBuffer.wrap(new byte[]{(byte) 0x80, 0x12}), true, (long) 1));
      }},  "1"},
      {new ArrayList<SampleClass>() {{
        //Empty list
      }}, "0"},
      {new ArrayList<SampleClass>() {{
        //Empty list
      }}, "abc"}
    });
  }

  @Before
  public void config() throws IOException {
    Properties props = System.getProperties();
    props.setProperty("org.apache.avro.limits.bytes.maxLength", maxBytesLenght);

    DatumWriter<SampleClass> writer = new SpecificDatumWriter<>(SampleClass.class);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    encoder = EncoderFactory.get().binaryEncoder(stream, null);

    for (SampleClass sampleClass : sampleClassList) {
      writer.write(sampleClass, encoder);
    }

    encoder.flush();

    data = stream.toByteArray();
    editedData = new byte[data.length];

    Random rd = new Random();
    byte[] arr = new byte[1];

    for(int i = 0; i < data.length; i++) {
      rd.nextBytes(arr);
      editedData[i] = (byte) (data[i] & (arr[0]));
    }

    reader = new SpecificDatumReader<>(SampleClass.class);
  }

  @Test
  public void binaryDecoderReadTest() throws IOException {
    int maxLengh;

    try {
      maxLengh = Integer.parseInt(maxBytesLenght);
    } catch (NumberFormatException e) {
      maxLengh = Integer.MAX_VALUE;
    }

    decoder = DecoderFactory.get().binaryDecoder(data, null);
    for (SampleClass sampleClass : sampleClassList) {
      try {
        SampleClass out = reader.read(null, decoder);
        Assert.assertEquals(sampleClass, out);
      } catch (AvroRuntimeException e) {
        int bytesLen = sampleClass.getBytes().capacity();
        Assert.assertTrue(bytesLen > maxLengh);
      }

    }
  }

  @Test
  public void binaryDecoderRandomizedData() throws IOException {
    decoder = DecoderFactory.get().binaryDecoder(editedData, null);
    for (SampleClass sampleClass : sampleClassList) {
      try {
        SampleClass out = reader.read(null, decoder);
        Assert.assertNotEquals(sampleClass, out);
      } catch (Exception e) {
        Assert.assertTrue(true);
      }
      //Assert.assertThrows(Exception.class, () -> {reader.read(null, decoder);});
    }
  }

  @Test
  public void isEndTest() throws IOException {
    int maxLengh;

    try {
      maxLengh = Integer.parseInt(maxBytesLenght);
    } catch (NumberFormatException e) {
      maxLengh = Integer.MAX_VALUE;
    }

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

    if (sampleClassList.isEmpty()) {
      Assert.assertTrue(decoder.isEnd());
      return;
    }

    for (int i = 0; i < sampleClassList.size(); i++) {
      try {
        reader.read(null, decoder);
      } catch (AvroRuntimeException e) {
        int bytesLen = sampleClassList.get(i).getBytes().capacity();
        Assert.assertTrue(bytesLen > maxLengh);
        return;
      }

      if (i < sampleClassList.size() - 1) {
        Assert.assertFalse(decoder.isEnd());
      } else {
        Assert.assertTrue(decoder.isEnd());
      }
    }
  }

}
