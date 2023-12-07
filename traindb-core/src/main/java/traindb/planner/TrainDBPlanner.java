/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.planner;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.runtime.Hook;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Holder;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import traindb.adapter.jdbc.JdbcConvention;
import traindb.adapter.jdbc.JdbcTableScan;
import traindb.adapter.jdbc.TrainDBJdbcTable;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogException;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MSynopsis;
import traindb.common.TrainDBConfiguration;
import traindb.planner.caqp.CaqpExecutionTimePolicy;
import traindb.planner.caqp.CaqpExecutionTimePolicyType;
import traindb.planner.rules.TrainDBRules;
import traindb.prepare.TrainDBCatalogReader;

public class TrainDBPlanner extends VolcanoPlanner {

  public static final double DEFAULT_SYNOPSIS_SIZE_RATIO = 0.01;

  private CatalogContext catalogContext;
  private TrainDBCatalogReader catalogReader;

  private CaqpExecutionTimePolicy caqpExecutionTimePolicy;

  public TrainDBPlanner(CatalogContext catalogContext, TrainDBCatalogReader catalogReader) {
    this(catalogContext, catalogReader, null, null);
  }

  public TrainDBPlanner(CatalogContext catalogContext,
                        TrainDBCatalogReader catalogReader,
                        @Nullable RelOptCostFactory costFactory,
                        @Nullable Context externalContext) {
    super(costFactory, externalContext);
    this.catalogContext = catalogContext;
    this.catalogReader = catalogReader;
    try {
      this.caqpExecutionTimePolicy = createCaqpExecutionTimePolicy(catalogReader.getConfig());
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    initPlanner();
  }

  public void initPlanner() {
    setNoneConventionHasInfiniteCost(false);

    // TrainDB rules
    //addRule(TrainDBRules.APPROX_AGGREGATE_SYNOPSIS_PROJECT_SCAN);
    //addRule(TrainDBRules.APPROX_AGGREGATE_SYNOPSIS_FILTER_SCAN);
    //addRule(TrainDBRules.APPROX_AGGREGATE_SYNOPSIS_AGGREGATE_SCAN);
    addRule(TrainDBRules.APPROX_AGGREGATE_SYNOPSIS);
    addRule(TrainDBRules.APPROX_AGGREGATE_INFERENCE);

    RelOptUtil.registerDefaultRules(this, true, Hook.ENABLE_BINDABLE.get(false));
    removeRule(CoreRules.FILTER_REDUCE_EXPRESSIONS);
    removeRule(CoreRules.AGGREGATE_CASE_TO_FILTER);
    removeRule(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
    addRule(TrainDBRules.AGGREGATE_REDUCE_FUNCTIONS);
    addRelTraitDef(ConventionTraitDef.INSTANCE);
    addRelTraitDef(RelCollationTraitDef.INSTANCE);
    setTopDownOpt(false);

    Hook.PLANNER.run(this); // allow test to add or remove rules
  }

  public TrainDBConfiguration getConfig() {
    return (TrainDBConfiguration) catalogReader.getConfig();
  }

  private CaqpExecutionTimePolicy createCaqpExecutionTimePolicy(CalciteConnectionConfig config)
      throws Exception {
    CaqpExecutionTimePolicyType policy;
    double unitAmount;
    if (config instanceof TrainDBConfiguration) {
      TrainDBConfiguration conf = (TrainDBConfiguration) config;
      policy = CaqpExecutionTimePolicyType.of(conf.getAqpExecTimePolicy());
      if (policy == null) {
        policy = CaqpExecutionTimePolicyType.getDefaultPolicy();
      }
      String unitAmountString = conf.getAqpExecTimeUnitAmount();
      if (unitAmountString == null) {
        unitAmount = policy.defaultUnitAmount;
      } else {
        unitAmount = Double.valueOf(unitAmountString);
      }
    } else {
      policy = CaqpExecutionTimePolicyType.getDefaultPolicy();
      unitAmount = policy.defaultUnitAmount;
    }
    return policy.policyClass.getDeclaredConstructor(double.class).newInstance(unitAmount);
  }

  public CatalogContext getCatalogContext() {
    return catalogContext;
  }

  public Collection<MSynopsis> getAvailableSynopses(List<String> qualifiedBaseTableName,
                                                    List<String> requiredColumnNames) {
    String baseSchema = qualifiedBaseTableName.get(1);
    String baseTable = qualifiedBaseTableName.get(2);
    try {
      Collection<MSynopsis> synopses = catalogContext.getAllSynopses(baseSchema, baseTable);
      List<MSynopsis> availableSynopses = new ArrayList<>();
      for (MSynopsis synopsis : synopses) {
        if (!synopsis.isEnabled()) {
          continue;
        }
        List<String> synopsisColumnNames = synopsis.getColumnNames();
        if (synopsisColumnNames.containsAll(requiredColumnNames)) {
          availableSynopses.add(synopsis);
        }
      }
      return availableSynopses;
    } catch (CatalogException e) {
    }
    return null;
  }

  public RelOptTable getSynopsisTable(MSynopsis synopsis, RelOptTable baseTable) {
    List<String> qualifiedSynopsisName = new ArrayList(Arrays.asList(
        baseTable.getQualifiedName().get(0), baseTable.getQualifiedName().get(1),
        synopsis.getSynopsisName()));
    double ratio = synopsis.getRatio();
    if (ratio == 0d) {
      ratio = DEFAULT_SYNOPSIS_SIZE_RATIO;
    }
    double rowCount = baseTable.getRowCount() * ratio;
    return catalogReader.getTable(qualifiedSynopsisName, rowCount);
  }

  public MSynopsis getBestSynopsis(Collection<MSynopsis> synopses, TableScan scan,
                                   List<RelHint> hints, List<String> requiredColumnNames) {
    Collection<MSynopsis> hintSynopses = new ArrayList<>();
    for (RelHint hint : hints) {
      if (hint.hintName.equals("synopsis")) {
        List<String> hintSynopsisNames = hint.listOptions;
        if (hintSynopsisNames.isEmpty()) {
          continue;
        }
        for (MSynopsis synopsis : synopses) {
          if (hintSynopsisNames.contains(synopsis.getSynopsisName())) {
            hintSynopses.add(synopsis);
          }
        }
        synopses = hintSynopses;
      }
    }

    Collection<MSynopsis> filteredSynopses = new HashSet<>();
    for (RelHint hint : hints) {
      try {
        if (hint.hintName.equals("approx_time")) {
          List<String> hintExecTime = hint.listOptions;
          for (String str : hintExecTime) {
            double hintTime = Double.valueOf(str);
            for (MSynopsis syn : synopses) {
              if (caqpExecutionTimePolicy.check(syn, hintTime)) {
                filteredSynopses.add(syn);
              }
            }
          }
        } else if (hint.hintName.equals("approx_error")) {
          List<String> hintErrors = hint.listOptions;
          for (String s : hintErrors) {
            double hintError = Double.valueOf(s) /* percent */ * 0.01;
            for (MSynopsis syn : synopses) {
              String synopsisStatistics = syn.getSynopsisStatistics();
              if (synopsisStatistics == null || synopsisStatistics.isEmpty()) {
                continue;
              }
              JSONParser parser = new JSONParser();
              JSONArray jsonColumnStats = (JSONArray) parser.parse(synopsisStatistics);
              double score = 0;
              for (int i = 0; i < jsonColumnStats.size(); i++) {
                JSONObject colstat = (JSONObject) jsonColumnStats.get(i);
                String columnName = (String) colstat.get("Column");
                if (requiredColumnNames.contains(columnName)) {
                  double qs = (Double) colstat.get("Quality Score");
                  if (score > 0) {
                    score = Double.min(score, qs);
                  } else {
                    score = qs;
                  }
                }
              }
              double errorEstimate = 1.0 - score;
              if (errorEstimate > hintError) {
                filteredSynopses.add(syn);
              }
            }
          }
        }
      } catch (Exception e) {
        // ignore
      }
    }
    if (filteredSynopses.size() < synopses.size()) {
      synopses.removeAll(filteredSynopses);
    }

    if (synopses.size() == 1) {
      return synopses.iterator().next();
    }
    MSynopsis bestSynopsis = null;
    double bestSynopsisCost = Double.MAX_VALUE;
    for (MSynopsis synopsis : synopses) {

      RelOptTableImpl synopsisTable =
          (RelOptTableImpl) getSynopsisTable(synopsis, scan.getTable());
      if (synopsisTable == null) {
        continue;
      }
      TableScan synopsisScan =
          new JdbcTableScan(scan.getCluster(), scan.getHints(), synopsisTable,
              (TrainDBJdbcTable) synopsisTable.table(), (JdbcConvention) scan.getConvention());

      RelMetadataQuery mq = scan.getCluster().getMetadataQuery();
      RelOptCost synopsisScanCost = mq.getCumulativeCost(synopsisScan);

      List<Double> synopsisScanColumnSizes = mq.getAverageColumnSizes(synopsisScan);
      Double synopsisScanColumnSize =
          synopsisScanColumnSizes.stream().mapToDouble(Double::doubleValue).sum();

      double cost =  synopsisScanCost.getRows() * synopsisScanColumnSize;
      if (cost < bestSynopsisCost) {
        bestSynopsis = synopsis;
        bestSynopsisCost = cost;
      }
    }
    return bestSynopsis;
  }

  public Collection<MModel> getAvailableInferenceModels(
      List<String> qualifiedBaseTableName, List<String> requiredColumnNames) {
    String baseSchema = qualifiedBaseTableName.get(1);
    String baseTable = qualifiedBaseTableName.get(2);
    try {
      Collection<MModel> models = catalogContext.getInferenceModels(baseSchema, baseTable);
      List<MModel> availableModels = new ArrayList<>();
      for (MModel model : models) {
        if (!model.isEnabled()) {
          continue;
        }
        List<String> columnNames = model.getColumnNames();
        if (columnNames.containsAll(requiredColumnNames)) {
          availableModels.add(model);
        }
      }
      return availableModels;
    } catch (CatalogException e) {
    }
    return null;
  }

  public Program getProgram() {
    Hook.REL_BUILDER_SIMPLIFY.addThread(Hook.propertyJ(false));
    // Allow a test to override the default program.
    final Holder<@Nullable Program> holder = Holder.empty();
    Hook.PROGRAM.run(holder);
    @Nullable Program holderValue = holder.get();
    if (holderValue != null) {
      return holderValue;
    }
    Program p = Programs.standard();

    final Program program1 =
        (planner, rel, requiredOutputTraits, materializations, lattices) -> {
          for (RelOptMaterialization materialization : materializations) {
            planner.addMaterialization(materialization);
          }
          for (RelOptLattice lattice : lattices) {
            planner.addLattice(lattice);
          }

          planner.setRoot(rel);
          final RelNode rootRel2 =
              rel.getTraitSet().equals(requiredOutputTraits)
                  ? rel
                  : planner.changeTraits(rel, requiredOutputTraits);
          assert rootRel2 != null;

          planner.setRoot(rootRel2);
          final RelOptPlanner planner2 = planner.chooseDelegate();
          final RelNode rootRel3 = planner2.findBestExp();
          assert rootRel3 != null : "could not implement exp";
          return rootRel3;
        };

    RelMetadataProvider metadataProvider = DefaultRelMetadataProvider.INSTANCE;
    return Programs.sequence(subQuery(metadataProvider),
        new DecorrelateProgram(),
        new TrimFieldsProgram(),
        program1,
        // Second planner pass to do physical "tweaks". This the first time
        // that EnumerableCalcRel is introduced.
        Programs.calc(metadataProvider));
  }

  public static Program subQuery(RelMetadataProvider metadataProvider) {
    final HepProgramBuilder builder = HepProgram.builder();
    builder.addRuleCollection(
        ImmutableList.of(CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
            CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE));
            //CoreRules.JOIN_SUB_QUERY_TO_CORRELATE));
    return Programs.of(builder.build(), true, metadataProvider);
  }

  /** Program that de-correlates a query. */
  private static class DecorrelateProgram implements Program {
    @Override public RelNode run(RelOptPlanner planner, RelNode rel,
                                 RelTraitSet requiredOutputTraits,
                                 List<RelOptMaterialization> materializations,
                                 List<RelOptLattice> lattices) {
      final CalciteConnectionConfig config =
          planner.getContext().maybeUnwrap(CalciteConnectionConfig.class)
              .orElse(CalciteConnectionConfig.DEFAULT);
      if (config.forceDecorrelate()) {
        final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
        return RelDecorrelator.decorrelateQuery(rel, relBuilder);
      }
      return rel;
    }
  }

  /** Program that trims fields. */
  private static class TrimFieldsProgram implements Program {
    @Override public RelNode run(RelOptPlanner planner, RelNode rel,
                                 RelTraitSet requiredOutputTraits,
                                 List<RelOptMaterialization> materializations,
                                 List<RelOptLattice> lattices) {
      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      return new RelFieldTrimmer(null, relBuilder).trim(rel);
    }
  }
}
