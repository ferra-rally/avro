package org.apache.avro;

import org.apache.avro.example.User;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;

public class BinaryDecoderTest {
  private Schema schema;
  private Encoder encoder;
  private Decoder decoder;

  @Before
  public void config() {
    /*
    schema = SchemaBuilder.record("User")
      .namespace("test.avro")
      .fields()
      .endRecord();*/

    //Decoder decoder = DecoderFactory.get().binaryDecoder();

  }

  @Test
  public void test() throws IOException {
    byte[] data = new byte[0];

    User user1 = new User();
    user1.setName("Alyssa");
    user1.setFavoriteNumber(256);

    User user2 = new User("Ben", 7, "red");

    DatumWriter<User> userDatumWriter = new SpecificDatumWriter<>(User.class);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();

    encoder = EncoderFactory.get().binaryEncoder(stream, null);

    userDatumWriter.write(user1, encoder);
    userDatumWriter.write(user2, encoder);

    encoder.flush();

    data = stream.toByteArray();

    DatumReader<User> reader = new SpecificDatumReader<>(User.class);

    decoder = DecoderFactory.get().binaryDecoder(data, null);

    User out = reader.read(null, decoder);
    Assert.assertEquals(user1, out);

    /*
    DataFileWriter<User> dataFileWriter = new DataFileWriter<>(userDatumWriter);
    dataFileWriter.create(User.getClassSchema(), new File("users.avro"));

    dataFileWriter.append(user1);
    dataFileWriter.append(user2);
    dataFileWriter.close();
    */

  }

}
