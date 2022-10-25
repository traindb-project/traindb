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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptCostFactory;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.runtime.Hook;
import org.checkerframework.checker.nullness.qual.Nullable;
import traindb.adapter.jdbc.JdbcConvention;
import traindb.adapter.jdbc.JdbcTableScan;
import traindb.adapter.jdbc.TrainDBJdbcTable;
import traindb.catalog.CatalogContext;
import traindb.catalog.CatalogException;
import traindb.catalog.pm.MModel;
import traindb.catalog.pm.MSynopsis;
import traindb.planner.rules.TrainDBRules;
import traindb.prepare.TrainDBCatalogReader;

public class TrainDBPlanner extends VolcanoPlanner {

  public static final double DEFAULT_SYNOPSIS_SIZE_RATIO = 0.01;

  private CatalogContext catalogContext;
  private TrainDBCatalogReader catalogReader;

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
    initPlanner();
  }

  public void initPlanner() {
    setNoneConventionHasInfiniteCost(false);

    // TrainDB rules
    addRule(TrainDBRules.APPROX_AGGREGATE_SYNOPSIS_PROJECT_SCAN);
    addRule(TrainDBRules.APPROX_AGGREGATE_SYNOPSIS_FILTER_SCAN);
    addRule(TrainDBRules.APPROX_AGGREGATE_SYNOPSIS_AGGREGATE_SCAN);
    addRule(TrainDBRules.APPROX_AGGREGATE_INFERENCE);

    RelOptUtil.registerDefaultRules(this, true, Hook.ENABLE_BINDABLE.get(false));
    addRelTraitDef(ConventionTraitDef.INSTANCE);
    addRelTraitDef(RelCollationTraitDef.INSTANCE);
    setTopDownOpt(false);

    Hook.PLANNER.run(this); // allow test to add or remove rules
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
        List<String> synopsisColumnNames = synopsis.getModel().getColumnNames();
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
        synopsis.getName()));
    double ratio = synopsis.getRatio();
    if (ratio == 0d) {
      ratio = DEFAULT_SYNOPSIS_SIZE_RATIO;
    }
    double rowCount = baseTable.getRowCount() * ratio;
    return catalogReader.getTable(qualifiedSynopsisName, rowCount);
  }

  public MSynopsis getBestSynopsis(Collection<MSynopsis> synopses, TableScan scan) {
    // TODO choose the best synopsis
    if (synopses.size() == 1) {
      return synopses.iterator().next();
    }
    MSynopsis bestSynopsis = null;
    double bestSynopsisScanRows = Double.valueOf(0.0d);
    double bestSynopsisScanCpu = Double.valueOf(0.0d);
    double bestSynopsisScanIo = Double.valueOf(0.0d);
    for (MSynopsis synopsis : synopses) {

      RelOptTableImpl synopsisTable =
          (RelOptTableImpl) getSynopsisTable(synopsis, scan.getTable());
      TableScan synopsisScan =
          new JdbcTableScan(scan.getCluster(), scan.getHints(), synopsisTable,
              (TrainDBJdbcTable) synopsisTable.table(), (JdbcConvention) scan.getConvention());

      RelMetadataQuery mq = scan.getCluster().getMetadataQuery();
      RelOptCost synopsisScanCost = mq.getCumulativeCost(synopsisScan);

      List<Double> synopsisScanColumnSizes = mq.getAverageColumnSizes(synopsisScan);
      Double synopsisScanColumnSize =
          synopsisScanColumnSizes.stream().mapToDouble(Double::doubleValue).sum();

      double synopsisScanRows = synopsisScanCost.getRows();
      double synopsisScanCpu = synopsisScanRows * synopsisScanColumnSize;
      double synopsisScanIo = synopsisScanCost.getIo();

      if ((bestSynopsis == null) || (synopsisScanRows < bestSynopsisScanRows) ||
          (synopsisScanCpu < bestSynopsisScanCpu) || (synopsisScanIo < bestSynopsisScanIo)) {
        bestSynopsis = synopsis;
        bestSynopsisScanRows = synopsisScanRows;
        bestSynopsisScanCpu = synopsisScanCpu;
        bestSynopsisScanIo = synopsisScanIo;
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

}