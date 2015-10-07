package org.agmip.ace.translator.input;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.healthmarketscience.jackcess.Column;
import com.healthmarketscience.jackcess.Cursor;
import com.healthmarketscience.jackcess.CursorBuilder;
import com.healthmarketscience.jackcess.Database;
import com.healthmarketscience.jackcess.DatabaseBuilder;
import com.healthmarketscience.jackcess.Index;
import com.healthmarketscience.jackcess.Row;
import com.healthmarketscience.jackcess.Table;

import org.agmip.ace.AceDataset;
import org.agmip.ace.AceWeather;
import org.agmip.ace.AceRecord;
import org.agmip.ace.AceRecordCollection;
import org.agmip.ace.translator.input.IToAceDataset;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EcofiToAceDataset implements IToAceDataset {
  private static final Logger LOG = LoggerFactory.getLogger(EcofiToAceDataset.class);

  public static AceDataset read(Path dbPath) {
    AceDataset ds = new AceDataset();
    Database db = null;
    dbPath = dbPath.toAbsolutePath();
    try {
      if (Files.isReadable(dbPath)) {
        db = new DatabaseBuilder(dbPath.toFile())
          .setReadOnly(true)
          .open();
        LOG.info("Translating ecofi file: {}", dbPath.toString());
      }
    }
    catch (IOException ex) {
      LOG.error("Unable to open file: {}, Reason: {}", dbPath.toString(), ex);
    }

    if (EcofiToAceDataset.hasWeatherTables(db)) {
      LOG.debug("Found the weather tables");
      EcofiToAceDataset.translateWeatherTables(db, ds);
    } else {
      LOG.debug("Could not find weather tables");
    }
    try {
      db.close();
    } catch (IOException ex) {
      LOG.error("IO Error: {}", ex);
    }
    return ds;
  }

  private static boolean hasWeatherTables(Database db) {
    boolean ws = false;
    boolean wdataday = false;
    try {
      Set<String> tbls = db.getTableNames();
      for( String tbl : tbls) {
        if (tbl.equals("ws")) {
          ws = true;
        } else if (tbl.equals("wdataday")) {
          wdataday = true;
        }
      }
    } catch (IOException ex) {
      LOG.error("IO Error: {}", ex);
    }
    return ws && wdataday;
  }

  private static AceDataset translateWeatherTables(Database db, AceDataset ds) {
    Table ws = null;
    Table wdataday = null;
    Map<Integer, AceWeather> cache = new HashMap<Integer, AceWeather>();
    Map<String, String> stationMap = new HashMap<String, String>();
    Map<String, String> dailyMap   = new HashMap<String, String>();
    stationMap.put("wsname", "wst_name");
    stationMap.put("wslat", "wst_lat");
    stationMap.put("wslong", "wst_long");
    stationMap.put("wsalt", "wst_elev");
    stationMap.put("countrycode", "wst_loc_1"); // Needs lookup table 2->3 char
    stationMap.put("x_wgs84", "ecofi_x_wgs84");
    stationMap.put("y_wgs84", "ecogi_y_wgs84");
    stationMap.put("wstype", "ecofi_wstype"); // Will probably be promoted.
    dailyMap.put("weatherdate", "w_date");
    dailyMap.put("tmin", "tmin");
    dailyMap.put("tmax", "tmax");
    dailyMap.put("tmoy", "tavd");
    dailyMap.put("rhmin", "rhumd");
    dailyMap.put("rhmax", "rhuxd");
    dailyMap.put("rainfall", "rain");
    dailyMap.put("windtot", "wind");
    dailyMap.put("radiation", "srad");
    dailyMap.put("sunshine", "sunh");
    dailyMap.put("eto", "eto");
    dailyMap.put("rhmoy", "ecofi_rhmoy");
    dailyMap.put("windmax", "ecofi_windmax");
    dailyMap.put("grad", "ecofi_grad");
    try {
      ws = db.getTable("ws");
      wdataday = db.getTable("wdataday");
      Cursor dailyCursor = CursorBuilder.createCursor(wdataday);
      DateTimeFormatter dtf = ISODateTimeFormat.basicDate();
      if (ws != null && wdataday != null) {
        // Process the station information
        // Because the current implementation of Ecofi is not using FK
        // relationships, we cannot determine the mapping without a hack.
        for (Row r : ws) {
          AceWeather station = new AceWeather();
          LOG.debug("Row data: {}", r.toString());
          LOG.debug("Starting wid: {}", station.getId());
          Integer wstid = r.getInt("wscode");
          if (wstid != null) {
            station.update("wst_id", wstid.toString());
            for(Map.Entry<String, String> translate: stationMap.entrySet()) {
              Object val = r.get(translate.getKey());
              if (val != null) {
                station.update(translate.getValue(), val.toString(), true, true, false);
              }
            }
            // Now deal with the daily values (as if we had the
            // really relationship)
            dailyCursor.reset();
            AceRecordCollection daily = station.getDailyWeather();
            for (Row dr : dailyCursor.newIterable().addMatchPattern("wscode", wstid)) {
              AceRecord rec = new AceRecord();
              for(Map.Entry<String, String> translate: dailyMap.entrySet()) {
                Object val = dr.get(translate.getKey());
                if (val != null) {
                  switch (translate.getKey()) {
                    case "weatherdate":
                      DateTime dt = new DateTime(dr.getDate(translate.getKey()));
                      rec.update(translate.getValue(), dtf.print(dt), true, true, false);
                      break;
                    case "windtot":
                      Double wind = dr.getDouble(translate.getKey());
                      rec.update(translate.getValue(), Double.valueOf(wind.doubleValue()*86.4).toString(), true, true, false);
                      break;
                    default:
                      rec.update(translate.getValue(), val.toString(), true, true, false);
                      break;
                  }
                }
              }
              daily.add(rec);
            }
            LOG.debug("Ending wid: {}", station.getId(true));
            LOG.debug(new String(station.rebuildComponent(), "UTF-8"));
            ds.addWeather(station.rebuildComponent());
          }
        }
      } else {
        LOG.error("Unable to process data. Unable to find ws and wsdataday tables");
      }
    } catch (IOException ex) {
      LOG.error("Error reading tables: {}", ex);
    } catch (IllegalArgumentException ex) {
      LOG.error("Unable to process data. Unable to find relationship between weather tables");
    }
    return ds;
  }
}
