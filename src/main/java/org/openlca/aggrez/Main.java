package org.openlca.aggrez;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Objects;

import org.openlca.core.database.Derby;
import org.openlca.core.database.upgrades.Upgrades;
import org.openlca.util.Dirs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {

    // collect input database and prepare the target database
    var input = inputFileOf(args);
    if (input == null)
      return;
    var target = targetFileOf(args);
    if (target == null)
      return;
    var dbDir = extractDatabase();
    if (dbDir == null)
      return;

    try (var targetDB = new Derby(dbDir)) {
      log.info("check database template");
      Upgrades.on(targetDB);
      log.info("transmitting flows");
      // TODO
      log.info("transmitting impact assessment methods");
      // TODO
      log.info("aggregating processes");
      // TODO
      log.info("transmitting processes");
      // TODO
      log.info("updating product systems");
      // TODO
      log.info("transmitting product systems");
      // TODO

    } catch (Exception e) {
      log.error("CSV import failed");
      return;
    }

    try {
      log.info("export database to: {}", target);
      Derby.zip(dbDir, target);
      log.info("drop temporary database: {}", dbDir);
      Dirs.delete(dbDir);
      log.info("all done");
    } catch (Exception e) {
      log.error("failed to finish zolca file", e);
    }
  }

  private static File extractDatabase() {
    try {
      var zipStream = Objects.requireNonNull(
        Main.class.getResourceAsStream("db.zolca"));
      var dbZip = Files.createTempFile("_aggrezdb_", ".zolca");
      log.info("copy database template to: {}", dbZip);
      Files.copy(zipStream, dbZip, StandardCopyOption.REPLACE_EXISTING);
      var dbDir = Files.createTempDirectory("_aggrezdb_").toFile();
      log.info("extract database backup in: {}", dbDir);
      Derby.unzip(dbDir, dbZip.toFile());
      log.info("drop database template: {}", dbZip);
      Files.delete(dbZip);
      return dbDir;
    } catch (Exception e) {
      log.error("failed to extract database", e);
      return null;
    }
  }

  private static File inputFileOf(String[] args) {
    var input = args[0];
    if (!input.toLowerCase().endsWith(".zolca"))
        return null;
    var file = new File(input);
    if (!file.exists()) {
      log.error("input file '{}' doesn't exist", file);
      return null;
    }
    log.info("selected input file: {}", file);
    return file;
  }

  private static File targetFileOf(String[] args) {
    var target = args[1];
    if (!target.toLowerCase().endsWith(".zolca"))
      return null;
    var file = new File(target);
    if (file.exists()) {
      log.error("target file '{}' already exists", file);
      return null;
    }
    log.info("selected target file: {}", file);
    return file;
  }

}
