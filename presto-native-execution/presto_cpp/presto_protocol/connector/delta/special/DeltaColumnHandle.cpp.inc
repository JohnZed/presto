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

namespace facebook::presto::protocol::delta {

DeltaColumnHandle::DeltaColumnHandle() noexcept {
  _type = "hive-delta";
}

void to_json(json& j, const DeltaColumnHandle& column) {
  j = json::object();
  j["@type"] = "hive-delta";
  to_json_key(j, "id", column.id);
  to_json_key(j, "physicalName", column.physicalName);
  to_json_key(j, "columnName", column.logicalName);
  to_json_key(j, "dataType", column.dataType);
  to_json_key(j, "columnType", column.columnType);
  to_json_key(j, "subfield", column.subfield);
}

void from_json(const json& j, DeltaColumnHandle& column) {
  column._type = j.at("@type");
  from_json_key(j, "id", column.id);
  from_json_key(j, "physicalName", column.physicalName);
  from_json_key(j, "columnName", column.logicalName);
  from_json_key(j, "dataType", column.dataType);
  from_json_key(j, "columnType", column.columnType);
  from_json_key(j, "subfield", column.subfield);
}

} // namespace facebook::presto::protocol::delta
