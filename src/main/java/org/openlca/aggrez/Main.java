package org.openlca.aggrez;

import java.io.File;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.openlca.core.database.Derby;
import org.openlca.core.database.upgrades.Upgrades;
import org.openlca.core.math.SystemCalculator;
import org.openlca.core.model.*;
import org.openlca.core.model.Process;
import org.openlca.core.results.SystemProcess;
import org.openlca.util.Dirs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {

    // collect input database and create the target database
    var input = inputFileOf(args);
    if (input == null)
      return;
    var target = targetFileOf(args);
    if (target == null)
      return;
    var referenceDate = referenceDateOf(args);
    if (referenceDate == null)
      return;
    var inputDbDir = extractDatabase(input);
    if (inputDbDir == null)
      return;
    var targetDbDir = createDatabase();
    if (targetDbDir == null)
      return;

    // init databases
    var inputDb = new Derby(inputDbDir);
    var targetDb = new Derby(targetDbDir);
    log.info("check input database");
    Upgrades.on(inputDb);
    Upgrades.on(targetDb);

    // aggregate processes
    log.info("aggregate processes");
    var unitToSystemMap = new HashMap<String, Process>();
    var modifiedUnitProcesses = new ArrayList<Process>();
    var calculator = new SystemCalculator(inputDb);
    var processes = inputDb.allOf(Process.class);
    for (var process : processes) {
      if (process.lastChange > referenceDate.getTime()) {
        modifiedUnitProcesses.add(process);
        continue;
      }
      log.trace("aggregate process: {}", process.refId);
      var setup = CalculationSetup.simple(process);
      var results = calculator.calculateSimple(setup);
      var systemProcess = SystemProcess.create(
              inputDb,
              setup,
              results,
              process.name
      );
      unitToSystemMap.put(process.refId, systemProcess);
    }

    //update product systems
    log.info("update product systems");
    var newProductSystems = new ArrayList<ProductSystem>();
    var productSystems = inputDb.allOf(ProductSystem.class);
    for (var productSystem : productSystems) {
      var referenceProcess = productSystem.referenceProcess;
      if (unitToSystemMap.containsKey(referenceProcess.refId))
        productSystem.referenceProcess = unitToSystemMap.get(referenceProcess.refId);
      var newProcessIds = new HashSet<Long>();
      for (var processId : productSystem.processes) {
        var process = inputDb.get(Process.class, processId);
        if (unitToSystemMap.containsKey(process.refId)) {
          var newProcess = unitToSystemMap.get(process.refId);
          newProcessIds.add(newProcess.id);
        } else {
          newProcessIds.add(processId);
        }
      }
      productSystem.processes.clear();
      productSystem.processes.addAll(newProcessIds);
      var newProcessLinks = new ArrayList<ProcessLink>();
      for (var processLink : productSystem.processLinks) {
        if (processLink.hasProcessProvider()) {
          var providerId = processLink.providerId;
          var process = inputDb.get(Process.class, providerId);
          if (unitToSystemMap.containsKey(process.refId)) {
            var newProcess = unitToSystemMap.get(process.refId);
            processLink.providerId = newProcess.id;
          }
        }
        var linkedProcess = inputDb.get(Process.class, processLink.processId);
        if (unitToSystemMap.containsKey(linkedProcess.refId)) {
          var newProcess = unitToSystemMap.get(linkedProcess.refId);
          processLink.processId = newProcess.id;
        }
        newProcessLinks.add(processLink);
      }
      productSystem.processLinks.clear();
      productSystem.processLinks.addAll(newProcessLinks);
      newProductSystems.add(productSystem);
    }

    // import data
    log.info("transmit reference data");
    transmit(UnitGroup.class, inputDb, targetDb);
    transmit(FlowProperty.class, inputDb, targetDb);

    log.info("transmit flows");
    transmit(Flow.class, inputDb, targetDb);

    log.info("transmit impact assessment methods");
    transmit(ImpactMethod.class, inputDb, targetDb);
    transmit(ImpactCategory.class, inputDb, targetDb);

    log.info("transmit global parameters");
    transmit(Parameter.class, inputDb, targetDb);

    log.info("transmit processes");
    for (var systemProcess : unitToSystemMap.values()) {
      log.trace("Transmitting Process {}", systemProcess.refId);
      targetDb.insert(systemProcess);
    }
    for (var process : modifiedUnitProcesses) {
      log.trace("Transmitting Process {}", process.refId);
      targetDb.insert(process);
    }

    log.info("transmit product systems");
    for (var productSystem : newProductSystems) {
      log.trace("Transmitting ProductSystem {}", productSystem.refId);
      targetDb.insert(productSystem);
    }

    try {
      log.info("export database to: {}", target);
      Derby.zip(targetDbDir, target);
      log.info("drop temporary database: {}", inputDbDir);
      Dirs.delete(inputDbDir);
      log.info("drop temporary database: {}", targetDbDir);
      Dirs.delete(targetDbDir);
      log.info("all done");
    } catch (Exception e) {
      log.error("failed to finish zolca file", e);
    }
  }

  private static <T extends RootEntity> void transmit(Class<T> entities, Derby from, Derby to) {
    var inputEntities = from.allOf(entities);
    if (inputEntities.isEmpty())
      return;
    for (var entity : inputEntities) {
      log.trace("Transmitting " + entities.getName() + " {}", entity.refId);
      to.insert(entity);
    }
  }

  private static File extractDatabase(File backupFile) {
    try {
      var dbDir = Files.createTempDirectory(backupFile.getName()).toFile();
      log.info("extract database in: {}", dbDir);
      Derby.unzip(dbDir, backupFile);
      return dbDir;
    } catch (Exception e) {
      log.error("failed to extract database", e);
      return null;
    }
  }
  private static File createDatabase() {
    try {
      var dbDir = Files.createTempDirectory("_aggrez_").toFile();
      log.info("create database in: {}", dbDir);
      return dbDir;
    } catch (Exception e) {
      log.error("failed to create database", e);
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

  private static Date referenceDateOf(String[] args) {
    try {
      var formatter = new SimpleDateFormat("dd.MM.yyyy");
      var date = formatter.parse(args[2]);
      log.info("selected reference date: {}", date.toString());
      return date;
    } catch (ParseException e) {
      log.error("failed to parse reference date", e);
      return null;
    }
  }
}
