/*
 * Copyright 2019 Rackspace US, Inc.
 *
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

package com.rackspace.salus.acm.web.model;

import com.rackspace.salus.telemetry.entities.AgentRelease;
import com.rackspace.salus.telemetry.model.AgentType;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import lombok.Data;

@Data
public class AgentReleaseDTO {
  UUID id;
  AgentType type;
  String version;
  Map<String,String> labels;
  String url;
  String exe;
  String createdTimestamp;
  String updatedTimestamp;

  public AgentReleaseDTO(AgentRelease agentRelease) {
    this.id = agentRelease.getId();
    this.type = agentRelease.getType();
    this.version = agentRelease.getVersion();
    this.labels = agentRelease.getLabels();
    this.url = agentRelease.getUrl();
    this.exe = agentRelease.getExe();
    this.createdTimestamp = DateTimeFormatter.ISO_INSTANT.format(agentRelease.getCreatedTimestamp());
    this.updatedTimestamp = DateTimeFormatter.ISO_INSTANT.format(agentRelease.getUpdatedTimestamp());
  }
}
