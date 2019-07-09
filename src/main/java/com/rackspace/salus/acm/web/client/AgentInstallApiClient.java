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

package com.rackspace.salus.acm.web.client;

import com.rackspace.salus.acm.web.model.BoundAgentInstallDTO;
import com.rackspace.salus.telemetry.model.AgentType;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public class AgentInstallApiClient implements AgentInstallApi {

  final RestTemplate restTemplate;

  public AgentInstallApiClient(RestTemplate restTemplate) {
    this.restTemplate = restTemplate;
  }

  @Override
  public BoundAgentInstallDTO getBindingForResourceAndAgentType(String tenantId, String resourceId, AgentType agentType) {
    final ResponseEntity<BoundAgentInstallDTO> response = restTemplate.exchange(
        "/api/admin/bound-agent-installs/{tenantId}/{resourceId}/{agentType}",
        HttpMethod.GET,
        null,
        BoundAgentInstallDTO.class,
        tenantId, resourceId, agentType
    );

    if (response.getStatusCode().equals(HttpStatus.NOT_FOUND)) {
      return null;
    } else {
      return response.getBody();
    }
  }
}
