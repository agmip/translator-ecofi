package org.agmip.ace.translator.input;

import java.io.ByteArrayOutputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;

import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.Test;

import org.agmip.ace.AceDataset;
import org.agmip.ace.io.AceGenerator;
//import org.agmip.ace.translator.input.EcofiToAceDataset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EcofiWeatherCheckTest {
  private static Logger LOG = LoggerFactory.getLogger(EcofiWeatherCheckTest.class);
  URL resource;

  @Before
  public void setUp() throws Exception {
    resource = this.getClass().getResource("/BdD_Louapre_Alexis_4aug.accdb");
  }

  @Test
  public void CheckDBForWeather() throws Exception {
    Path dbFile = Paths.get(resource.getPath());
    AceDataset ds = EcofiToAceDataset.read(dbFile);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    AceGenerator.generate(baos, ds, false);
    LOG.info("Number of weather stations: {}", ds.getWeathers().size());
  }
}
