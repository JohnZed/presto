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

#include "presto_cpp/main/connectors/DeltaPrestoToVeloxConnector.h"

#include "presto_cpp/main/connectors/PrestoToVeloxConnectorUtils.h"
#include "presto_cpp/presto_protocol/connector/delta/DeltaConnectorProtocol.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergSplit.h"
#include "velox/functions/prestosql/types/TimestampWithTimeZoneType.h"

namespace facebook::presto {
namespace {

const std::string& sourceName(
    const std::shared_ptr<std::string>& physicalName,
    const std::string& logicalName) {
  return physicalName ? *physicalName : logicalName;
}

std::unique_ptr<velox::connector::hive::HiveColumnHandle>
makeHiveColumnHandle(
    const std::string& name,
    bool isPartitionKey,
    const velox::TypePtr& type,
    std::vector<velox::common::Subfield> requiredSubfields = {}) {
  velox::connector::hive::HiveColumnHandle::ColumnParseParameters parameters{};
  if (type->isDate()) {
    parameters.partitionDateValueFormat = velox::connector::hive::
        HiveColumnHandle::ColumnParseParameters::kISO8601;
  }

  return std::make_unique<velox::connector::hive::HiveColumnHandle>(
      name,
      isPartitionKey
          ? velox::connector::hive::HiveColumnHandle::ColumnType::kPartitionKey
          : velox::connector::hive::HiveColumnHandle::ColumnType::kRegular,
      type,
      type,
      std::move(requiredSubfields),
      parameters);
}

bool canFilterOnFileValues(const velox::TypePtr& type) {
  return type->isPrimitiveType() && !type->isTimestamp() &&
      !velox::isTimestampWithTimeZoneType(type);
}

velox::common::SubfieldFilters toSubfieldFilters(
    const protocol::TupleDomain<protocol::delta::DeltaColumnHandle>& predicate,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) {
  velox::common::SubfieldFilters filters;
  if (predicate.domains == nullptr) {
    return filters;
  }

  for (const auto& [column, domain] : *predicate.domains) {
    if (column.columnType != protocol::delta::ColumnType::REGULAR) {
      continue;
    }
    const auto type = stringToType(column.dataType, typeParser);
    if (!canFilterOnFileValues(type)) {
      continue;
    }
    filters[velox::common::Subfield(
        sourceName(column.physicalName, column.logicalName))] =
        toFilter(domain, exprConverter, typeParser);
  }
  return filters;
}

bool isAbsolutePath(const std::string& path) {
  return (!path.empty() && path.front() == '/') ||
      path.find("://") != std::string::npos || path.starts_with("file:/");
}

std::string resolveFilePath(
    const std::string& tableLocation,
    const std::string& filePath) {
  if (isAbsolutePath(filePath)) {
    return filePath;
  }
  VELOX_USER_CHECK(!tableLocation.empty(), "Delta table location is empty");
  if (tableLocation.back() == '/') {
    return tableLocation + filePath;
  }
  return tableLocation + "/" + filePath;
}

} // namespace

std::unique_ptr<velox::connector::ConnectorSplit>
DeltaPrestoToVeloxConnector::toVeloxSplit(
    const protocol::ConnectorId& catalogId,
    const protocol::ConnectorSplit* connectorSplit,
    const protocol::SplitContext* splitContext) const {
  const auto* deltaSplit =
      dynamic_cast<const protocol::delta::DeltaSplit*>(connectorSplit);
  VELOX_CHECK_NOT_NULL(
      deltaSplit, "Unexpected split type {}", connectorSplit->_type);

  std::unordered_map<std::string, std::optional<std::string>> partitionKeys;
  partitionKeys.reserve(
      deltaSplit->partitionValues.size() +
      deltaSplit->nullPartitionKeys.size());
  for (const auto& [name, value] : deltaSplit->partitionValues) {
    partitionKeys.emplace(name, value);
  }
  for (const auto& name : deltaSplit->nullPartitionKeys) {
    VELOX_CHECK(
        partitionKeys.emplace(name, std::nullopt).second,
        "Partition key '{}' has both null and non-null values",
        name);
  }

  const auto fullFilePath =
      resolveFilePath(deltaSplit->tableLocation, deltaSplit->filePath);
  const std::unordered_map<std::string, std::string> customSplitInfo = {
      {"table_format", "delta"},
      {"schema", deltaSplit->schemaName},
      {"table", deltaSplit->tableName}};
  const std::unordered_map<std::string, std::string> infoColumns = {
      {"$path", fullFilePath},
      {"$file_size", std::to_string(deltaSplit->fileSize)}};

  // The Iceberg split subtype selects the existing native schema-adaptation
  // and GPU constant-injection path. No Iceberg metadata or delete files are
  // fabricated: Delta Kernel has already selected the active Parquet file.
  return std::make_unique<velox::connector::hive::iceberg::HiveIcebergSplit>(
      catalogId,
      fullFilePath,
      velox::dwio::common::FileFormat::PARQUET,
      deltaSplit->start,
      deltaSplit->length,
      partitionKeys,
      std::nullopt,
      customSplitInfo,
      nullptr,
      splitContext->cacheable,
      infoColumns);
}

std::unique_ptr<velox::connector::ColumnHandle>
DeltaPrestoToVeloxConnector::toVeloxColumnHandle(
    const protocol::ColumnHandle* column,
    const TypeParser& typeParser) const {
  const auto* deltaColumn =
      dynamic_cast<const protocol::delta::DeltaColumnHandle*>(column);
  VELOX_CHECK_NOT_NULL(
      deltaColumn, "Unexpected column handle type {}", column->_type);

  std::vector<velox::common::Subfield> requiredSubfields;
  if (deltaColumn->subfield) {
    requiredSubfields.emplace_back(*deltaColumn->subfield);
  }

  return makeHiveColumnHandle(
      sourceName(deltaColumn->physicalName, deltaColumn->logicalName),
      deltaColumn->columnType == protocol::delta::ColumnType::PARTITION,
      stringToType(deltaColumn->dataType, typeParser),
      std::move(requiredSubfields));
}

std::unique_ptr<velox::connector::ConnectorTableHandle>
DeltaPrestoToVeloxConnector::toVeloxTableHandle(
    const protocol::TableHandle& tableHandle,
    const VeloxExprConverter& exprConverter,
    const TypeParser& typeParser) const {
  const auto deltaTableHandle =
      std::dynamic_pointer_cast<const protocol::delta::DeltaTableHandle>(
          tableHandle.connectorHandle);
  VELOX_CHECK_NOT_NULL(deltaTableHandle, "Unexpected Delta table handle");

  std::vector<velox::connector::hive::HiveColumnHandlePtr> columnHandles;
  std::vector<std::string> dataColumnNames;
  std::vector<velox::TypePtr> dataColumnTypes;
  columnHandles.reserve(deltaTableHandle->deltaTable.columns.size());
  dataColumnNames.reserve(deltaTableHandle->deltaTable.columns.size());
  dataColumnTypes.reserve(deltaTableHandle->deltaTable.columns.size());

  for (const auto& column : deltaTableHandle->deltaTable.columns) {
    const auto name = sourceName(column.physicalName, column.logicalName);
    const auto type = stringToType(column.type, typeParser);
    columnHandles.emplace_back(
        makeHiveColumnHandle(name, column.partition, type));
    if (!column.partition) {
      dataColumnNames.emplace_back(name);
      dataColumnTypes.emplace_back(VELOX_DYNAMIC_TYPE_DISPATCH(
          fieldNamesToLowerCase, type->kind(), type));
    }
  }

  velox::common::SubfieldFilters subfieldFilters;
  if (tableHandle.connectorTableLayout != nullptr) {
    const auto layout = std::dynamic_pointer_cast<
        const protocol::delta::DeltaTableLayoutHandle>(
        tableHandle.connectorTableLayout);
    VELOX_CHECK_NOT_NULL(layout, "Unexpected Delta table layout handle");
    subfieldFilters =
        toSubfieldFilters(layout->predicate, exprConverter, typeParser);
  }

  const auto tableName = fmt::format(
      "{}.{}",
      deltaTableHandle->deltaTable.schemaName,
      deltaTableHandle->deltaTable.tableName);
  velox::RowTypePtr dataColumns;
  if (!dataColumnNames.empty()) {
    dataColumns = ROW(std::move(dataColumnNames), std::move(dataColumnTypes));
  }

  return std::make_unique<velox::connector::hive::HiveTableHandle>(
      tableHandle.connectorId,
      tableName,
      std::move(subfieldFilters),
      nullptr,
      dataColumns,
      std::unordered_map<std::string, std::string>{},
      columnHandles);
}

std::unique_ptr<protocol::ConnectorProtocol>
DeltaPrestoToVeloxConnector::createConnectorProtocol() const {
  return std::make_unique<protocol::delta::DeltaConnectorProtocol>();
}

} // namespace facebook::presto
