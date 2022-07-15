package org.openlca.aggrez;

import java.io.File;
import java.nio.file.Files;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.openlca.core.database.*;
import org.openlca.core.database.upgrades.Upgrades;
import org.openlca.core.math.SystemCalculator;
import org.openlca.core.model.*;
import org.openlca.core.model.Currency;
import org.openlca.core.model.Process;
import org.openlca.core.results.SystemProcess;
import org.openlca.util.Dirs;
import org.openlca.util.ParameterRedefSets;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

  private static final Logger log = LoggerFactory.getLogger(Main.class);
  private static Derby inputDb;
  private static Derby targetDb;
  private static long referenceDate = 0;

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
        case "-src" -> input = inputFileOf(arg);
        case "-dest" -> target = targetFileOf(arg);
        case "-from" -> referenceDate = referenceDateOf(arg);
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
    try {
      log.info("filter out unused processes");
      var processesToAggregate = new ArrayList<Process>();
      var unmodifiedProcesses = new ArrayList<Process>();
      var productSystems = inputDb.allOf(ProductSystem.class);
      var usedProcesses = productSystems
          .stream()
          .map(ps -> ps.referenceProcess)
          .collect(Collectors.toSet());
      usedProcesses = getForegroundProcesses(usedProcesses, usedProcesses);
      log.info("there are {} foreground processes", usedProcesses.size());
      var backgroundProcesses = getFirstBackgroundProcessesOf(usedProcesses);
      usedProcesses.addAll(backgroundProcesses);
      log.info("there are {} total used processes", usedProcesses.size());

      // classify processes
      log.info("there are {} processes to aggregate", backgroundProcesses.size());
      for (var process : usedProcesses) {
        if (process.processType == ProcessType.LCI_RESULT) {
          unmodifiedProcesses.add(process);
          continue;
        }
        if (process.lastChange > referenceDate) {
          unmodifiedProcesses.add(process);
          continue;
        }
        processesToAggregate.add(process);
      }

      // import reference data
      log.info("transmit reference data");
      transmitReferenceCategories();
      var requirePropertyUpdate = new HashMap<String, UnitGroup>();
      transmitUnitGroups(requirePropertyUpdate);
      transmit(FlowProperty.class);
      updateUnitGroups(requirePropertyUpdate);
      transmit(Location.class);
      var currencies = inputDb.allOf(Currency.class);
      var referenceCurrency = currencies
          .stream()
          .filter(c -> c.referenceCurrency == c)
          .findFirst()
          .orElseThrow();
      targetDb.insert(referenceCurrency);
      for (var currency : currencies) {
        if (currency == referenceCurrency)
          continue;
        targetDb.insert(currency);
      }

      // import global parameters
      log.info("transmit global parameters");
      transmitGlobalParameters();

      // import processes
      log.info("Transmit unmodified processes");
      for (var process : unmodifiedProcesses) {
        log.info("Transmitting Process {}", process.refId);
        process.dqEntry = null;
        process.dqSystem = null;
        process.exchangeDqSystem = null;
        process.socialDqSystem = null;
        process.documentation = null;
        transmitProcess(process);
      }
      log.info("Aggregating and transmitting background processes");
      for (var process : processesToAggregate) {
        log.info("Transmitting Process {}", process.refId);
        transmitProcess(aggregate(process));
      }

      // filter out unused impact factors
      log.info("filter out unused impact factors");
      var impactCategories = inputDb.allOf(ImpactCategory.class);
      var usedElementaryFlows = targetDb
              .allOf(Flow.class)
              .stream()
              .filter(f -> f.flowType == FlowType.ELEMENTARY_FLOW)
              .collect(Collectors.toList());
      impactCategories.forEach(impactCategory -> {
        var newImpactFactors = impactCategory.impactFactors
                .stream()
                .filter(factor -> usedElementaryFlows.contains(factor.flow))
                .collect(Collectors.toList());
        impactCategory.impactFactors.clear();
        impactCategory.impactFactors.addAll(newImpactFactors);
      });

      log.info("transmit impact assessment methods");
      for (var impactCategory : impactCategories) {
        targetDb.insert(impactCategory);
      }
      transmit(ImpactMethod.class);

      // update and transmit product systems
      log.info("transmit product systems");
      var usedProcessIds = Stream.concat(
          unmodifiedProcesses.stream(),
          processesToAggregate.stream())
          .map(p -> p.id)
          .collect(Collectors.toSet());
      var systemProcessIds = processesToAggregate
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
                return !(systemProcessIds.contains(pl.providerId)
                    && systemProcessIds.contains(pl.processId));
              return true;
            })
            .filter(pl -> {
              if (pl.hasProcessProvider())
                return productSystem.processes.contains(pl.providerId)
                    && productSystem.processes.contains(pl.processId);
              return productSystem.processes.contains(pl.processId);
            })
            .collect(Collectors.toList());
        productSystem.processLinks.clear();
        productSystem.processLinks.addAll(processLinks);
        productSystem.parameterSets.add(ParameterRedefSets.allOf(targetDb, productSystem));
        log.trace("Transmitting ProductSystem {}", productSystem.refId);
        targetDb.insert(productSystem);
      }

    // Backup database
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
      Dirs.delete(inputDbDir);
      Dirs.delete(targetDbDir);
      log.error("failed to finish zolca file", e);
    }
  }

  private static Process aggregate(Process process) {
    var setup = CalculationSetup.simple(process);
    var calc = new SystemCalculator(inputDb);
    var result = calc.calculateSimple(setup);
    var systemProcess = SystemProcess.create(
            inputDb,
            setup,
            result,
            process.name);
    systemProcess.id = process.id;
    systemProcess.refId = process.refId;
    systemProcess.category = process.category;
    systemProcess.description = process.description;
    systemProcess.location = process.location;
    systemProcess.lastChange = new Date().getTime();
    return systemProcess;
  }

  private static <T extends RootEntity> void transmit(Class<T> entities) {
    var inputEntities = inputDb.allOf(entities);
    if (inputEntities.isEmpty())
      return;
    for (var entity : inputEntities) {
      log.info("Transmitting " + entities.getName() + " {}", entity.refId);
      targetDb.insert(entity);
    }
  }

  private static void transmitProcess(Process process) {
    transmitCategoriesOf(process);
    transmitFlowsOf(process);
    targetDb.insert(process);
  }

  private static void transmitCategoriesOf(CategorizedEntity entity) {
    if (entity.category == null) return;
    var categoryPath = getParentCategories(
            entity.category,
            new ArrayList<>()
    );
    Collections.reverse(categoryPath);
    for (var cat : categoryPath) {
      var check = targetDb.get(Category.class, cat.refId);
      if (check == null) {
        log.info("Transmitting Category {} ", cat.refId);
        targetDb.insert(cat);
      }
    }
  }

  private static void transmitFlowsOf(Process process) {
    var processFlows = process.exchanges
            .stream()
            .map(e -> e.flow)
            .collect(Collectors.toSet());
    for (var flow : processFlows) {
      var check = targetDb.get(Flow.class, flow.refId);
      if (check == null) {
        transmitCategoriesOf(flow);
        log.info("Transmitting Flow {} ", flow.refId);
        targetDb.insert(flow);
      }
    }
  }

  private static void transmitReferenceCategories() {
    var sourceDao = new CategoryDao(inputDb);
    var targetDao = new CategoryDao(targetDb);
    for (var modelType : ModelType.categorized()) {
      switch (modelType) {
        case UNIT_GROUP, FLOW_PROPERTY, LOCATION, CURRENCY, PARAMETER, IMPACT_CATEGORY, IMPACT_METHOD,
                PRODUCT_SYSTEM -> {
          var sourceRoots = sourceDao.getRootCategories(modelType);
          for (var sourceRoot : sourceRoots) {
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

  private static List<Category> getParentCategories(Category category, List<Category> acc) {
    if (!acc.contains(category)) {
      acc.add(category);
    }
    var parentCategory = category.category;
    if (parentCategory == null) return acc;
    acc.add(parentCategory);
    return getParentCategories(parentCategory, acc);
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
