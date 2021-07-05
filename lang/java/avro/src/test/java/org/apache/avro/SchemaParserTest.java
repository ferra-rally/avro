package org.apache.avro;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.example.SampleClass;
import org.apache.avro.example.User;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;

@RunWith(value = Parameterized.class)
public class SchemaParserTest {
  private File file;
  private Schema parseSchema;
  private JsonGenerator generator;
  private String jsonString;
  private boolean expected;

  public SchemaParserTest(Schema parseSchema, String jsonString, Boolean expected) {
    this.parseSchema = parseSchema;
    this.jsonString = jsonString;
    this.expected = expected;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {SampleClass.getClassSchema(), null, true},
      {SchemaBuilder.record("testing.schemas").fields()
        .name("enumTest").type().nullable().enumeration("aname")
        .symbols("a","b","c","d","e").noDefault().endRecord(), null, true},
      {SchemaBuilder.record("testing.schemas").fields()
        .name("enumTest").type().nullable().enumeration("aname")
        .symbols("a","b","c","d","e").noDefault().endRecord(), "{\"type\":\"as\",\"name\":\"schemas\",\"namespace\":\"testing\",\"fields\":[{\"name\":\"enumTest\",\"type\":[{\"type\":\"enum\",\"name\":\"aname\",\"symbols\":[\"a\",\"b\",\"c\",\"d\",\"e\"]},\"null\"]}]}", false},
      {SchemaBuilder.unionOf().doubleType().and().intType().endUnion(), null, true},
      {SchemaBuilder.enumeration("enumarion").symbols("a", "n"), null, true},
      {SchemaBuilder.array().items(SchemaBuilder.builder().intType()), null, true},
      {SchemaBuilder.map().values(SchemaBuilder.builder().booleanType()), null, true}
    });
  }

  @Before
  public void config() throws IOException {
    file = File.createTempFile("stream", ".tmp");

    JsonFactory factory = new JsonFactory();
    generator = factory.createGenerator(file, JsonEncoding.UTF8);
  }

  @Test
  public void parseJsonTest() throws IOException {
    String out = "";
    if(jsonString == null) {
      Schema.Names names = new Schema.Names();

      //Parse the schema to a json string
      parseSchema.toJson(names, generator);
      generator.close();
      InputStream inputStream = new FileInputStream(file);

      byte[] dest = new byte[(int) file.length()];
      int read = inputStream.read(dest);
      if (read > 0) {
        out = new String(dest);

        Assert.assertEquals(parseSchema.toString(), out);
      } else {
        Assert.fail();
      }
    }
    //Check if the parse is able to get the schema back
    Schema.Parser parser = new Schema.Parser();

    if(jsonString != null) {
      Assert.assertThrows(SchemaParseException.class, () -> {parser.parse(jsonString);});
    } else {
      boolean result = parseSchema.equalCachedHash(parser.parse(out));

      Assert.assertEquals(expected, result);
    }

  }
}
