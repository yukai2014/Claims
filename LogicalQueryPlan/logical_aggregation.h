/*
 * Copyright [2012-2015] DaSE@ECNU
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * ./LogicalPlan/logical_aggregation.h
 *
 *  Created on: Nov 11,2013
 *      Author: wangli, fangzhuhe
 *       Email: fzhedu@gmail.com
 *
 * Description: Aggregation operator is designed for executing group by and
 * aggregation function. LogicalAggregation is the logical type of Aggregation
 * operator, main function includes getting data context after executing this
 * operator and generating corresponding physical operator.
 */
#ifndef LOGICALQUERYPLAN_LOGICAL_AGGREGATION_H_
#define LOGICALQUERYPLAN_LOGICAL_AGGREGATION_H_
#include <vector>
#include "LogicalOperator.h"
#include "../Catalog/Attribute.h"
#include "../BlockStreamIterator/ParallelBlockStreamIterator/BlockStreamAggregationIterator.h"

// namespace claims {
// namespace logical_query_plan {
/**
 * @brief class description: maintain operator property that results from
 * executing
 * aggregation operator, and generate corresponding physical operator.
 * @details as for the implementation of avg() based on sum()/count(), so it's
 * necessary to change the output_schema of private aggregation in
 * kLocalAggReparGlobalAgg aggregation style.
 * Note: kReparAndGlobalAgg node isn't implemented.
 */

class LogicalAggregation : public LogicalOperator {
 public:
  /**
   * AggregationStyle describe the implementation of aggregation judged from
   * whether the data distribution agrees with the group by attributes or the
   * data is local.
   */
  enum AggregationStyle {
    kAgg,  // the data is local or it's distribution agrees with the group
           // by attributes, one aggregation is enough.
    kReparGlobalAgg,         // otherwise first repartition raw data and then
                             // aggregation
    kLocalAggReparGlobalAgg  // additionally, first do local aggregation
    // based on raw data, second repartition the intermediate
    // result, and last do global aggregation.
  };

  /**
   * This is the default construct method of Aggregation.
   * This logical operator creates a new dataflow.
   * group_by_attribute can be empty if it is scalar aggregation.
   * count(*) aggregation can be expressed as 'count' aggregation function along
   * with Attribute(ATTRIBUTE_ANY)
   */
  LogicalAggregation(
      std::vector<Attribute> group_by_attribute_list,
      std::vector<Attribute> aggregation_attribute_list,
      std::vector<BlockStreamAggregationIterator::State::aggregation>
          aggregation_function_list, LogicalOperator* child);
  virtual ~LogicalAggregation();

 protected:
  /**
   * get data context resulting from executing aggregation operator.
   * including corresponding attribute_list and property with partition info and
   * communication cost.
   */
  Dataflow GetDataflow();
  /**
   * generate corresponding physical plan, mainly set necessary state.
   */
  BlockStreamIteratorBase* GetIteratorTree(const unsigned& block_size);

 private:
  std::vector<unsigned> GetInvolvedAttrIdList(
      const std::vector<Attribute>& attribute_list,
      const Dataflow& dataflow) const;
  bool CanOmitHashRepartition(const Dataflow& child_dataflow) const;
  float EstimateSelectivity() const;
  /**
   * the kLocalAggReparGlobalAgg aggregation schema of local aggregation may be
   * different from global aggregation's.
   * For instance, to achieve count(*) aggregation, the local is count(*) while
   * the global one should be sum(*) to guarantee the correctness.
   * and in the output schema of local aggregation, the avg() column should be
   * changed to sum() and append additional count() at the end.
   */

  // TODO(fzh): merge ChangeSchemaForAVG() and ChangeForGlobalAggregation() and
  // not change but construct the schema during reorganizing physical operator.

  void ChangeSchemaForAVG(BlockStreamAggregationIterator::State& state);
  std::vector<BlockStreamAggregationIterator::State::aggregation>
  ChangeForGlobalAggregation(const std::vector<
      BlockStreamAggregationIterator::State::aggregation>) const;
  void Print(int level = 0) const;

  /**
   * generate a list containing attributes that are generated by the aggregation
   * operator.
   */
  std::vector<Attribute> GetAttrsAfterAgg() const;
  std::vector<Attribute> GetGroupByAttrsAfterAgg() const;
  std::vector<Attribute> GetAggAttrsAfterAgg() const;

  unsigned long EstimateGroupByCardinality(const Dataflow& dataflow) const;

  LogicalOperator* child_;
  std::vector<Attribute> group_by_attribute_list_;
  std::vector<Attribute> aggregation_attribute_list_;
  std::vector<BlockStreamAggregationIterator::State::aggregation>
      aggregation_function_list_;
  Dataflow* dataflow_;
  AggregationStyle aggregation_style_;
};
//} // namespace logical_query_plan
//}  // namespace claims
#endif  // LOGICALQUERYPLAN_LOGICAL_AGGREGATION_H_
