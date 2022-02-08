package org.openlca.aggrez;

import java.io.File;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import org.openlca.core.database.*;
import org.openlca.core.database.upgrades.Upgrades;
import org.openlca.core.math.CalculationQueue;
import org.openlca.core.model.*;
import org.openlca.core.model.Process;
import org.openlca.core.results.SimpleResult;
import org.openlca.core.results.SystemProcess;
import org.openlca.util.Dirs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);

  public static void main(String[] args) {

    // init default args
    File input = null;
    File target = null;
    long referenceDate = 0;
    int threads = 2;

    // parse the provided args
    String flag = null;
    for (var arg : args) {
      if (arg.startsWith("-")) {
        flag = arg;
        continue;
      }
      if (flag == null)
        continue;
      switch (flag) {
        case "-src":
          input = inputFileOf(arg);
          if (input == null)
            return;
          break;
        case "-dest":
          target = targetFileOf(arg);
          if (target == null)
            return;
          break;
        case "-from":
          referenceDate = referenceDateOf(arg);
          break;
        case "-threads":
          try {
            threads = Integer.parseInt(arg);
          } catch (NumberFormatException e) {
            log.error("invalid number of threads provided: {}", arg);
            return;
          }
          break;
      }
    }

    // collect input database and create the target database
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
    var calculationQueue = new CalculationQueue(inputDb, threads);
    var calcToSetupMap = new HashMap<String, CalculationSetup>();
    var unitToSystemMap = new HashMap<String, Process>();
    var unmodifiedProcesses = new ArrayList<Process>();
    var productSystemDao = new ProductSystemDao(inputDb);
    var productSystems = productSystemDao.getAll();
    var foregroundProcesses = productSystems
            .stream()
            .map(ps -> ps.referenceProcess)
            .collect(Collectors.toSet());
    var usedProcesses = getUsedProcesses(foregroundProcesses, foregroundProcesses, inputDb);
    for (var process : usedProcesses) {
      if (process.processType == ProcessType.LCI_RESULT) {
        unmodifiedProcesses.add(process);
        continue;
      }
      if (process.lastChange > referenceDate) {
        unmodifiedProcesses.add(process);
        continue;
      }
      var setup = CalculationSetup.simple(process);
      var calcState = calculationQueue.schedule(setup);
      calcToSetupMap.put(calcState.id(), setup);
    }
    for (var calcId : calcToSetupMap.keySet()) {
      var isReady = false;
      CalculationQueue.State state = null;
      while (!isReady) {
        state = calculationQueue.get(calcId);
        isReady = state.isReady();
      }
      var results = (SimpleResult) state.result();
      var process = calcToSetupMap.get(calcId).process();
      log.trace("aggregate process: {}", process.refId);
      var systemProcess = SystemProcess.create(
              inputDb,
              calcToSetupMap.get(calcId),
              results,
              process.name
      );
      unitToSystemMap.put(process.refId, systemProcess);
    }

    //update product systems
    log.info("update product systems");
    var newProductSystems = new ArrayList<ProductSystem>();
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
    transmitCategories(inputDb, targetDb);
    var requirePropertyUpdate = new HashMap<String, UnitGroup>();
    transmitUnitGroups(inputDb, targetDb, requirePropertyUpdate);
    transmit(FlowProperty.class, inputDb, targetDb);
    updateUnitGroups(targetDb, requirePropertyUpdate);

    log.info("transmit global parameters");
    transmitGlobalParameters(inputDb, targetDb);

    log.info("transmit flows");
    transmit(Flow.class, inputDb, targetDb);

    log.info("transmit impact assessment methods");
    transmit(ImpactCategory.class, inputDb, targetDb);
    transmit(ImpactMethod.class, inputDb, targetDb);

    log.info("transmit processes");
    for (var systemProcess : unitToSystemMap.values()) {
      log.trace("Transmitting Process {}", systemProcess.refId);
      targetDb.insert(systemProcess);
    }
    for (var process : unmodifiedProcesses) {
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

  private static <T extends RootEntity> void transmit(Class<T> entities, Derby source, Derby target) {
    var inputEntities = source.allOf(entities);
    if (inputEntities.isEmpty())
      return;
    for (var entity : inputEntities) {
      log.trace("Transmitting " + entities.getName() + " {}", entity.refId);
      target.insert(entity);
    }
  }

  private static void transmitCategories(Derby source, Derby target) {
    var sourceDao = new CategoryDao(source);
    var targetDao = new CategoryDao(target);
    var sourceRoots = sourceDao.getRootCategories();
    for (var sourceRoot : sourceRoots) {
      log.trace("Transmitting Categories {}", sourceRoot.refId);
      targetDao.insert(sourceRoot);
    }
  }

  private static void transmitUnitGroups(Derby source, Derby target, HashMap<String, UnitGroup> requireUpdate) {
    var sourceDao = new UnitGroupDao(source);
    var targetDao = new UnitGroupDao(target);
    var sourceUnitGroups = sourceDao.getAll();
    for (var sourceUnitGroup : sourceUnitGroups) {
      var targetUnitGroup = clone(sourceUnitGroup);
      if (sourceUnitGroup.defaultFlowProperty != null) {
        targetUnitGroup.defaultFlowProperty = null;
        requireUpdate.put(sourceUnitGroup.defaultFlowProperty.refId, targetUnitGroup);
      }
      log.trace("Transmitting UnitGroup {}", targetUnitGroup.refId);
      targetDao.insert(targetUnitGroup);
    }
  }

  private static void updateUnitGroups(Derby target, HashMap<String, UnitGroup> requireUpdate) {
    var propertyDao = new FlowPropertyDao(target);
    var unitGroupDao = new UnitGroupDao(target);
    for (String refId : requireUpdate.keySet()) {
      var unitGroup = requireUpdate.get(refId);
      unitGroup.defaultFlowProperty = propertyDao.getForRefId(refId);
      unitGroupDao.update(unitGroup);
    }
  }

  private static void transmitGlobalParameters(Derby source, Derby target) {
    var sourceDao = new ParameterDao(source);
    var targetDao = new ParameterDao(target);
    for (var parameter : sourceDao.getGlobalParameters()) {
      if (parameter.name == null)
        continue;
      log.trace("Transmitting Parameter {}", parameter.refId);
      targetDao.insert(parameter);
    }
  }

  private static Set<Process> getUsedProcesses(Set<Process> foregroundProcesses, Set<Process> usedProcesses, Derby db) {
    var inputProcesses = foregroundProcesses
            .stream()
            .flatMap(p -> p.exchanges.stream())
            .filter(e -> e.defaultProviderId != 0)
            .filter(e -> e.isInput)
            .filter(e -> e.flow.flowType != FlowType.ELEMENTARY_FLOW)
            .map(e -> db.get(Process.class, e.defaultProviderId))
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    inputProcesses.removeIf(usedProcesses::contains);
    if (inputProcesses.isEmpty())
      return usedProcesses;
    usedProcesses.addAll(inputProcesses);
    return getUsedProcesses(inputProcesses, usedProcesses, db);
  }

  private static UnitGroup clone(UnitGroup from) {
    var clone = new UnitGroup();
    clone.id = from.id;
    clone.refId = from.refId;
    clone.name = from.name;
    clone.description = from.description;
    clone.version = from.version;
    clone.lastChange = from.lastChange;
    clone.category = from.category;
    clone.tags = from.tags;
    clone.library = from.library;
    clone.defaultFlowProperty = from.defaultFlowProperty;
    for (var unit : from.units) {
      var clonedUnit = unit.copy();
      clonedUnit.id = unit.id;
      clonedUnit.refId = unit.refId;
      clone.units.add(clonedUnit);
      if (Objects.equals(from.referenceUnit, unit)) {
        clone.referenceUnit = clonedUnit;
      }
    }
    return clone;
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

  private static File inputFileOf(String arg) {
    if (!arg.toLowerCase().endsWith(".zolca"))
      return null;
    var file = new File(arg);
    if (!file.exists()) {
      log.error("input file '{}' doesn't exist", file);
      return null;
    }
    log.info("selected input file: {}", file);
    return file;
  }

  private static File targetFileOf(String arg) {
    if (!arg.toLowerCase().endsWith(".zolca"))
      return null;
    var file = new File(arg);
    if (file.exists()) {
      log.error("target file '{}' already exists", file);
      return null;
    }
    log.info("selected target file: {}", file);
    return file;
  }

  private static long referenceDateOf(String arg) {
    try {
      var formatter = new SimpleDateFormat("dd.MM.yyyy");
      var date = formatter.parse(arg);
      log.info("selected reference date: {}", date.toString());
      return date.getTime();
    } catch (ParseException e) {
      log.error("failed to parse reference date", e);
      return 0;
    }
  }
}
