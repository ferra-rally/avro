package org.apache.avro;

import org.apache.avro.example.SampleClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.*;

@RunWith(value = Parameterized.class)
public class SchemaTest {
  private String field_name;
  private List<String> aliasList;
  private List<String> enumFields;

  public SchemaTest(String field_name, List<String> aliasList, List<String> enumFields) {
    this.field_name = field_name;
    this.aliasList = aliasList;
    this.enumFields = enumFields;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {"test_field", new ArrayList<String>() {{add("alias1"); add("alias2");}}, new ArrayList<String>() {{  add("A"); add("B");}}}
    });
  }

  @Before
  public void config() {

  }
  @Test
  public void schemaEnumTest() {

    Schema schema = Schema.createEnum(field_name, "", "", enumFields);

    Assert.assertEquals(enumFields, schema.getEnumSymbols());
  }

  @Test
  public void aliasTest() {
    Schema schema =  Schema.createEnum(field_name, "", "", enumFields);

    for(String alias : aliasList) {
      schema.addAlias(alias);
    }

    Set<String> aliases = schema.getAliases();

    for(String name : aliases) {
      boolean contains = aliasList.contains(name);
      Assert.assertTrue(contains);
    }
  }
}
