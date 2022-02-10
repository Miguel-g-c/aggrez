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
import org.openlca.core.model.Currency;
import org.openlca.core.model.Process;
import org.openlca.core.results.SimpleResult;
import org.openlca.core.results.SystemProcess;
import org.openlca.util.Dirs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static Derby inputDb;
  private static Derby targetDb;
  private static long referenceDate = 0;
  private static int threads = 2;

  public static void main(String[] args) {

    // init default args
    File input = null;
    File target = null;

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
          break;
        case "-dest":
          target = targetFileOf(arg);
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
    if (input == null)
      return;
    if (target == null)
      return;
    if (referenceDate == 0)
      return;
    var inputDbDir = extractDatabase(input);
    if (inputDbDir == null)
      return;
    var targetDbDir = createDatabase();
    if (targetDbDir == null)
      return;

    // init databases
    inputDb = new Derby(inputDbDir);
    targetDb = new Derby(targetDbDir);
    log.info("check input database");
    Upgrades.on(inputDb);
    Upgrades.on(targetDb);

    // filter out unused processes
    log.info("filter out unused processes");
    var calculationQueue = new CalculationQueue(inputDb, threads);
    var calcToSetupMap = new HashMap<String, CalculationSetup>();
    var systemProcesses = new ArrayList<Process>();
    var unmodifiedProcesses = new ArrayList<Process>();
    var productSystems = inputDb.allOf(ProductSystem.class);
    var usedProcesses = productSystems
            .stream()
            .map(ps -> ps.referenceProcess)
            .collect(Collectors.toSet());
    usedProcesses = getForegroundProcesses(usedProcesses, usedProcesses);
    var backgroundProcesses = getFirstBackgroundProcessesOf(usedProcesses);
    usedProcesses.addAll(backgroundProcesses);

    // filter out unused technosphere flows
    log.info("filter out unused technosphere flows");
    var usedProductFlows = usedProcesses
            .stream()
            .flatMap(p -> p.exchanges.stream())
            .map(e -> e.flow)
            .filter(f -> f.flowType != FlowType.ELEMENTARY_FLOW)
            .collect(Collectors.toSet());

    // filter out unused categories
    log.info("filter out unused categories");
    var usedProcessCategories = usedProcesses
            .stream()
            .map(p -> p.category)
            .collect(Collectors.toSet());
    usedProcessCategories = getParentCategories(usedProcessCategories, usedProcessCategories);

    // aggregate processes
    log.info("aggregate processes");
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
      systemProcess.id = process.id;
      systemProcess.refId = process.refId;
      systemProcess.category = process.category;
      systemProcess.description = process.description;
      systemProcess.location = process.location;
      systemProcess.lastChange = new Date().getTime();
      systemProcesses.add(systemProcess);
      calculationQueue.dispose(calcId);
    }
    var executor = calculationQueue.shutdown();
    executor.shutdownNow();

    // import data
    log.info("transmit reference data");
    transmitCategories(usedProcessCategories);
    var requirePropertyUpdate = new HashMap<String, UnitGroup>();
    transmitUnitGroups(requirePropertyUpdate);
    transmit(FlowProperty.class);
    updateUnitGroups(requirePropertyUpdate);
    transmit(Location.class);
    transmit(Currency.class);

    log.info("transmit global parameters");
    transmitGlobalParameters();

    log.info("transmit flows");
    transmitFlows(usedProductFlows);

    log.info("transmit impact assessment methods");
    transmit(ImpactCategory.class);
    transmit(ImpactMethod.class);

    log.info("transmit processes");
    for (var process : unmodifiedProcesses) {
      log.trace("Transmitting Process {}", process.refId);
      process.dqEntry = null;
      process.dqSystem = null;
      process.exchangeDqSystem = null;
      process.socialDqSystem = null;
      process.documentation = null;
      targetDb.insert(process);
    }

    for (var process : systemProcesses) {
      log.trace("Transmitting Process {}", process.refId);
      targetDb.insert(process);
    }

    // update and transmit product systems
    log.info("transmit product systems");
    var usedProcessIds = usedProcesses
            .stream()
            .map(p -> p.id)
            .collect(Collectors.toSet());
    for (var productSystem : productSystems) {
      var processIds = productSystem.processes
              .stream()
              .filter(usedProcessIds::contains)
              .collect(Collectors.toSet());
      productSystem.processes.clear();
      productSystem.processes.addAll(processIds);
      var processLinks = productSystem.processLinks
               .stream()
               .filter(pl -> {
                 if (pl.hasProcessProvider())
                   return usedProcessIds.contains(pl.providerId);
                 return usedProcessIds.contains(pl.processId);
               })
              .collect(Collectors.toList());
      productSystem.processLinks.clear();
      productSystem.processLinks.addAll(processLinks);
      log.trace("Transmitting ProductSystem {}", productSystem.refId);
      targetDb.insert(productSystem);
    }

    // Backup database
    try {
      log.info("export database to: {}", target);
      Derby.zip(targetDbDir, target);
      log.info("drop temporary database: {}", inputDbDir);
      inputDb.close();
      Dirs.delete(inputDbDir);
      log.info("drop temporary database: {}", targetDbDir);
      targetDb.close();
      Dirs.delete(targetDbDir);
      log.info("all done");
    } catch (Exception e) {
      log.error("failed to finish zolca file", e);
    }
  }

  private static <T extends RootEntity> void transmit(Class<T> entities) {
    var inputEntities = inputDb.allOf(entities);
    if (inputEntities.isEmpty())
      return;
    for (var entity : inputEntities) {
      log.trace("Transmitting " + entities.getName() + " {}", entity.refId);
      targetDb.insert(entity);
    }
  }

  private static void transmitCategories(Set<Category> processCategories) {
    var sourceDao = new CategoryDao(inputDb);
    var targetDao = new CategoryDao(targetDb);
    for (var modelType : ModelType.categorized()) {
      switch (modelType) {
        case UNIT_GROUP, FLOW_PROPERTY, LOCATION, CURRENCY, PARAMETER, IMPACT_CATEGORY, IMPACT_METHOD, FLOW, PRODUCT_SYSTEM -> {
          var sourceRoots = sourceDao.getRootCategories(modelType);
          for (var sourceRoot : sourceRoots) {
            log.trace("Transmitting Categories {}", sourceRoot.refId);
            targetDao.insert(sourceRoot);
          }
        }
        case PROCESS -> {
          var sourceRoots = sourceDao.getRootCategories(modelType);
          for (var sourceRoot : sourceRoots) {
            if (!processCategories.contains(sourceRoot))
              continue;
            filterOutUnusedCategoriesOf(sourceRoot, processCategories);
            log.trace("Transmitting Categories {}", sourceRoot.refId);
            targetDao.insert(sourceRoot);
          }
        }
      }
    }
  }

  private static void transmitUnitGroups(HashMap<String, UnitGroup> requireUpdate) {
    var sourceDao = new UnitGroupDao(inputDb);
    var targetDao = new UnitGroupDao(targetDb);
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

  private static void updateUnitGroups(HashMap<String, UnitGroup> requireUpdate) {
    var propertyDao = new FlowPropertyDao(targetDb);
    var unitGroupDao = new UnitGroupDao(targetDb);
    for (String refId : requireUpdate.keySet()) {
      var unitGroup = requireUpdate.get(refId);
      unitGroup.defaultFlowProperty = propertyDao.getForRefId(refId);
      unitGroupDao.update(unitGroup);
    }
  }

  private static void transmitGlobalParameters() {
    var sourceDao = new ParameterDao(inputDb);
    var targetDao = new ParameterDao(targetDb);
    for (var parameter : sourceDao.getGlobalParameters()) {
      if (parameter.name == null)
        continue;
      log.trace("Transmitting Parameter {}", parameter.refId);
      targetDao.insert(parameter);
    }
  }

  private static void transmitFlows(Set<Flow> productFlows) {
    var sourceDao = new FlowDao(inputDb);
    var targetDao = new FlowDao(targetDb);
    var elementaryFlows = sourceDao.getAll()
            .stream()
            .filter(f -> f.flowType == FlowType.ELEMENTARY_FLOW)
            .collect(Collectors.toList());
    for (var flow : elementaryFlows) {
      targetDao.insert(flow);
    }
    for (var flow : productFlows) {
      targetDao.insert(flow);
    }
  }

  private static Set<Process> getForegroundProcesses(Set<Process> processes, Set<Process> foregroundProcesses) {
    var inputProcesses = processes
            .stream()
            .flatMap(p -> p.exchanges.stream())
            .filter(e -> e.defaultProviderId != 0)
            .filter(e -> e.flow.flowType != FlowType.ELEMENTARY_FLOW)
            .filter(e -> e.isInput || e.flow.flowType == FlowType.WASTE_FLOW)
            .map(e -> inputDb.get(Process.class, e.defaultProviderId))
            .filter(Objects::nonNull)
            .filter(p -> p.lastChange > referenceDate)
            .collect(Collectors.toSet());
    inputProcesses.removeIf(foregroundProcesses::contains);
    if (inputProcesses.isEmpty())
      return foregroundProcesses;
    foregroundProcesses.addAll(inputProcesses);
    return getForegroundProcesses(inputProcesses, foregroundProcesses);
  }

  private static Set<Process> getFirstBackgroundProcessesOf(Set<Process> processes) {
    return processes
            .stream()
            .flatMap(p -> p.exchanges.stream())
            .filter(e -> e.defaultProviderId != 0)
            .filter(e -> e.flow.flowType != FlowType.ELEMENTARY_FLOW)
            .filter(e -> e.isInput || e.flow.flowType == FlowType.WASTE_FLOW)
            .map(e -> inputDb.get(Process.class, e.defaultProviderId))
            .filter(Objects::nonNull)
            .filter(p -> p.lastChange < referenceDate)
            .collect(Collectors.toSet());
  }

  private static Set<Category> getParentCategories(Set<Category> categories, Set<Category> usedCategories) {
    var parentCategories = categories
            .stream()
            .map(c -> c.category)
            .filter(Objects::nonNull)
            .collect(Collectors.toSet());
    parentCategories.removeIf(usedCategories::contains);
    if (parentCategories.isEmpty())
      return usedCategories;
    usedCategories.addAll(parentCategories);
    return getParentCategories(parentCategories, usedCategories);
  }

  private static void filterOutUnusedCategoriesOf(Category category, Set<Category> usedCategories) {
    if (category.childCategories.isEmpty())
      return;
    var newChildren = category.childCategories
            .stream()
            .filter(usedCategories::contains)
            .collect(Collectors.toList());
    if (newChildren.isEmpty()) {
      category.childCategories.clear();
      return;
    }
    category.childCategories.clear();
    category.childCategories.addAll(newChildren);
    for (var cat : category.childCategories) {
      filterOutUnusedCategoriesOf(cat, usedCategories);
    }
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
