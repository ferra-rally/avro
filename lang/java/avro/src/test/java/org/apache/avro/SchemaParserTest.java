package org.apache.avro;

import com.fasterxml.jackson.databind.JsonNode;
import org.apache.avro.example.SampleClass;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.*;

@RunWith(value = Parameterized.class)
public class SchemaParserTest {

  private Schema parseSchema;
  private String jsonString;
  private SpecificRecord specificRecord;

  public SchemaParserTest(Schema parseSchema, SpecificRecord specificRecord) {
    this.parseSchema = parseSchema;
    this.specificRecord = specificRecord;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {SampleClass.getClassSchema(), new SampleClass("string", null, 1,(float) 1.0, 2.0, ByteBuffer.wrap(new byte[] { (byte)0x80, 0x53}), true, (long) 1)}
    });
  }

  @Before
  public void config() {
    jsonString = specificRecord.toString();
  }

  @Test
  public void parseJsonToObectTest() {
    LinkedHashMap out = (LinkedHashMap) Schema.parseJsonToObject(jsonString);

    System.out.println(out.values());
    System.out.println(specificRecord);

    System.out.println(out.equals(specificRecord));

    //Check if attribute names remained the same
    List<Schema.Field> fields = specificRecord.getSchema().getFields();
    Set keyset = out.keySet();

    for(int i = 0; i < fields.size(); i++) {
      Schema.Field field = fields.get(i);

      //Check names
      boolean hasName = keyset.contains(field.name());
      Assert.assertTrue(hasName);

      //Check values
      if(specificRecord.get(i) != null) {
        //Avro converts floats to double
        if(!field.schema().equalCachedHash(SchemaBuilder.builder().floatType())) {
          Assert.assertEquals(specificRecord.get(i), out.get(field.name()));
        }
        //TODO check with delta
        /*else {
          //Check with delta
          float floa = (float) specificRecord.get(i);
          System.out.println(floa);
        }*/
      }
    }

    //Assert.assertEquals(specificRecord, out);
  }

  @Test
  public void parseJsonTest() {
    JsonNode jNode = Schema.parseJson(parseSchema.toString());
    Schema.Names names = new Schema.Names();

    boolean result = parseSchema.equalCachedHash(Schema.parse(jNode, names));

    Assert.assertTrue(result);
  }
}
