package org.apache.avro;

import org.apache.avro.example.SampleClass;
import org.apache.avro.example.User;
import org.apache.avro.specific.SpecificRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.*;

@RunWith(value = Parameterized.class)
public class SchemaParseJsonObjectTest {
  private String jsonString;
  private SpecificRecord specificRecord;

  public SchemaParseJsonObjectTest(SpecificRecord specificRecord) {
    this.specificRecord = specificRecord;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      { (new SampleClass("string", null, 1,(float) 1.0, 2.0, ByteBuffer.wrap(new byte[] { (byte)0x32, 0x53, 0x53}), true, (long) 1))},
      { (new User("name", 23, "red"))},
      {new User()}
    });
  }

  @Before
  public void config() {
    if(specificRecord != null) {
        jsonString = specificRecord.toString();
    }
  }

  @Test
  public void parseJsonToObectTest() {
    LinkedHashMap out = (LinkedHashMap) Schema.parseJsonToObject(jsonString);

    if(jsonString.isEmpty()) {
      Assert.assertNull(out);
    } else {

      //Check if attribute names remained the same
      List<Schema.Field> fields = specificRecord.getSchema().getFields();
      Set keyset = out.keySet();

      for (int i = 0; i < fields.size(); i++) {
        Schema.Field field = fields.get(i);

        //Check names
        boolean hasName = keyset.contains(field.name());
        Assert.assertTrue(hasName);

        //Check values
        if (specificRecord.get(i) != null) {
          //Avro converts floats to double
          if (field.schema().isUnion()) {
            //If it is a union
            if (field.schema().getTypes().contains(SchemaBuilder.builder().floatType())) {
              Double record = Double.parseDouble(out.get(field.name()).toString());
              Assert.assertEquals(record, out.get(field.name()));
            } else if (field.schema().getTypes().contains(SchemaBuilder.builder().bytesType())) {
              ByteBuffer buffer = (ByteBuffer) specificRecord.get(i);

              String outString = out.get(field.name()).toString();
              Assert.assertEquals(new String(buffer.array()), outString);
            } else {
              Assert.assertEquals(specificRecord.get(i).toString(), out.get(field.name()).toString());
            }
          } else {
            Assert.assertEquals(specificRecord.get(i), out.get(field.name()));
          }
        }
      }
    }
  }
}
