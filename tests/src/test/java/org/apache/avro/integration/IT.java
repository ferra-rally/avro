package org.apache.avro.integration;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.compiler.specific.SpecificCompiler;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class IT {

  @Test
  public void test() throws IOException {
    File outputRoot = Files.createTempDirectory(null).toFile();

    Schema schema = SchemaBuilder.record("testing.schemas").fields()
      .name("enumTest").type().nullable().enumeration("aname")
      .symbols("a","b","c","d","e").noDefault().endRecord();

    SpecificCompiler compiler = new SpecificCompiler(schema);

    compiler.compileToDestination(null, outputRoot);
    File javaFile = new File(outputRoot, schema.getNamespace().replaceAll("\\.",File.separator) + File.separator + schema.getName() + ".java");
    Assert.assertTrue(javaFile.exists());

    /*
    String sourceCode;
    try (FileInputStream fis = new FileInputStream(javaFile)) {
      sourceCode = IOUtils.toString(fis, StandardCharsets.UTF_8);
    }*/
  }
}
