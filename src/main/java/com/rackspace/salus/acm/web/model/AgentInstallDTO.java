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

import com.fasterxml.jackson.annotation.JsonView;
import com.rackspace.salus.telemetry.entities.AgentInstall;
import com.rackspace.salus.telemetry.model.LabelSelectorMethod;
import com.rackspace.salus.telemetry.model.View;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.UUID;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class AgentInstallDTO {
  UUID id;

  @JsonView(View.Admin.class)
  String tenantId;

  AgentReleaseDTO agentRelease;

  Map<String, String> labelSelector;

  LabelSelectorMethod labelSelectorMethod;

  String createdTimestamp;
  String updatedTimestamp;

  public AgentInstallDTO(AgentInstall agentInstall) {
    this.id = agentInstall.getId();
    this.agentRelease = new AgentReleaseDTO(agentInstall.getAgentRelease());
    this.tenantId = agentInstall.getTenantId();
    this.labelSelector = agentInstall.getLabelSelector();
    this.createdTimestamp = DateTimeFormatter.ISO_INSTANT.format(agentInstall.getCreatedTimestamp());
    this.updatedTimestamp = DateTimeFormatter.ISO_INSTANT.format(agentInstall.getUpdatedTimestamp());
    this.labelSelectorMethod = agentInstall.getLabelSelectorMethod();
  }
}
