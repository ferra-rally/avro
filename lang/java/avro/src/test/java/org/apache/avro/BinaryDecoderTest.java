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
  private Decoder decoder;
  private List<SampleClass> sampleClassList;
  private boolean aBoolean;
  private byte[] data;
  private DatumReader<SampleClass> reader;

  public BinaryDecoderTest(List<SampleClass> sampleClassList, boolean aBoolean) {
    this.sampleClassList = sampleClassList;
    this.aBoolean = aBoolean;
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
        add(new SampleClass("string", null, 1,(float) 1.0, 1.0, null, true, (long) 1));
        add(new SampleClass("", null, 100,(float) 0, 1.0, null, false, (long) 1));
      }}, true},
      {new ArrayList<SampleClass>() {{
        add(new SampleClass("string", null, 1,(float) 1.0, 1.0, ByteBuffer.wrap(new byte[] { (byte)0x80, 0x53}), true, (long) 1));
      }}, false},
      {new ArrayList<SampleClass>() {{
        //Empty list
      }}, false}
    });
  }

  @Before
  public void config() throws IOException {
    //TODO edit java properties
    /*
    Properties props = System.getProperties();
    props.setProperty("org.apache.avro.limits.bytes.maxLength", "3");
    System.out.println(props.getProperty("org.apache.avro.limits.bytes.maxLength"));
*/
    DatumWriter<SampleClass> writer = new SpecificDatumWriter<>(SampleClass.class);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    encoder = EncoderFactory.get().binaryEncoder(stream, null);

    for(SampleClass sampleClass : sampleClassList) {
      writer.write(sampleClass, encoder);
    }

    encoder.flush();

    data = stream.toByteArray();

    reader = new SpecificDatumReader<>(SampleClass.class);
  }

  @Test
  public void binaryDecoderReadTest() throws IOException {
    decoder = DecoderFactory.get().binaryDecoder(data, null);

    for(SampleClass sampleClass : sampleClassList) {
      SampleClass out = reader.read(null, decoder);
      Assert.assertEquals(sampleClass, out);
    }
  }

  //TODO Move test to separate unit test
  @Test
  public void readBooleanTest() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
    encoder.writeBoolean(aBoolean);
    encoder.flush();

    data = stream.toByteArray();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

    boolean out = decoder.readBoolean();
    Assert.assertEquals(aBoolean, out);
  }
/*
  @Test
  public void skipArrayTest() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
    encoder.writeArrayStart();

    encoder.writeInt(3);
    encoder.writeInt(2);
    encoder.writeInt(1);
    encoder.writeArrayEnd();
    encoder.flush();

    data = stream.toByteArray();

    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
    //System.out.println(decoder.readArrayStart());
    decoder.skipArray();

    System.out.println(decoder.readInt());
    System.out.println(decoder.readInt());
  }*/

  @Test
  public void isEndTest() throws IOException {
    BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);

    if(sampleClassList.isEmpty()) {
      Assert.assertTrue(decoder.isEnd());
      return;
    }

    for(int i = 0; i < sampleClassList.size(); i++) {
      reader.read(null, decoder);

      if(i < sampleClassList.size() - 1) {
        Assert.assertFalse(decoder.isEnd());
      } else {
        Assert.assertTrue(decoder.isEnd());
      }
    }
  }

}
