package org.apache.avro.integration;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

@RunWith(value = Parameterized.class)
public class CompilerSchemaTest {
  private Schema schema;

  public CompilerSchemaTest(Schema schema) {
    this.schema = schema;
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getTestParameters() {
    return Arrays.asList(new Object[][]{
      {SchemaBuilder.record("testing.schemas").fields()
        .name("enumTest").type().nullable().enumeration("aname")
        .symbols("a","b","c","d","e").noDefault().endRecord()},
      {SchemaBuilder.record("testing.schemas").fields()
        .name("enumTest").type().nullable().enumeration("aname")
        .symbols("a","b","c","d","e").noDefault().endRecord()},
      {SchemaBuilder.record("testing.schemas").fields()
        .name("enumTest").type().nullable().enumeration("aname")
        .symbols("a","b","c","d","e").enumDefault("a").name("array").type().array().items(SchemaBuilder.builder().booleanType()).noDefault()
        .name("map").type().map().values(SchemaBuilder.builder().booleanType()).noDefault()
        .name("fixed").type().fixed("fixed").size(2).noDefault()
        .endRecord()}
    });
  }

  @Before
  public void configure() {

  }

  @Test
  public void test() throws IOException {
    File outputRoot = Files.createTempDirectory("out").toFile();

    SpecificCompiler compiler = new SpecificCompiler(schema);

    compiler.compileToDestination(null, outputRoot);
    File javaFile = new File("out/" + schema.getNamespace().replace("\\.", File.separator) + File.separator + schema.getName()+ ".java");

    Assert.assertTrue(javaFile.exists());
  }
}
