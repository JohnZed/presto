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

#include <gtest/gtest.h>

#include "presto_cpp/presto_protocol/connector/delta/DeltaConnectorProtocol.h"

namespace facebook::presto::protocol::delta {
namespace {

TEST(DeltaConnectorProtocolTest, splitJsonRoundTrip) {
  const json expected = {
      {"@type", "hive-delta"},
      {"connectorId", "delta"},
      {"schemaName", "default"},
      {"tableName", "events"},
      {"tableLocation", "s3://warehouse/default/events"},
      {"filePath", "part=2026-08-31/part-00000.parquet"},
      {"start", 0},
      {"length", 4096},
      {"fileSize", 4096},
      {"partitionValues", {{"empty", ""}, {"part", "2026-08-31"}}},
      {"nullPartitionKeys", {"nullable_part"}}};

  DeltaConnectorProtocol protocol;
  std::shared_ptr<ConnectorSplit> connectorSplit;
  protocol.from_json(expected, connectorSplit);

  const auto split = std::dynamic_pointer_cast<DeltaSplit>(connectorSplit);
  ASSERT_NE(split, nullptr);
  EXPECT_EQ(split->connectorId, "delta");
  EXPECT_EQ(split->tableLocation, "s3://warehouse/default/events");
  EXPECT_EQ(split->filePath, "part=2026-08-31/part-00000.parquet");
  EXPECT_EQ(split->partitionValues.at("empty"), "");
  EXPECT_EQ(split->partitionValues.at("part"), "2026-08-31");
  EXPECT_EQ(split->nullPartitionKeys, Set<String>({"nullable_part"}));

  json actual;
  protocol.to_json(actual, connectorSplit);
  EXPECT_EQ(actual, expected);
}

TEST(DeltaConnectorProtocolTest, transactionHandleJsonRoundTrip) {
  const json expected = {"hive-delta", "INSTANCE"};

  DeltaConnectorProtocol protocol;
  std::shared_ptr<ConnectorTransactionHandle> connectorHandle;
  protocol.from_json(expected, connectorHandle);

  const auto handle =
      std::dynamic_pointer_cast<DeltaTransactionHandle>(connectorHandle);
  ASSERT_NE(handle, nullptr);
  EXPECT_EQ(handle->_type, "hive-delta");
  EXPECT_EQ(handle->instance, "INSTANCE");

  json actual;
  protocol.to_json(actual, connectorHandle);
  EXPECT_EQ(actual, expected);
}

} // namespace
} // namespace facebook::presto::protocol::delta
