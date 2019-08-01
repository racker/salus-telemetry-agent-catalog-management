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

package com.rackspace.salus.acm.web.controller;

import com.fasterxml.jackson.annotation.JsonView;
import com.rackspace.salus.acm.entities.AgentInstall;
import com.rackspace.salus.acm.repositories.AgentInstallRepository;
import com.rackspace.salus.acm.repositories.BoundAgentInstallRepository;
import com.rackspace.salus.acm.services.AgentInstallService;
import com.rackspace.salus.acm.web.client.AgentInstallApi;
import com.rackspace.salus.acm.web.model.AgentInstallCreate;
import com.rackspace.salus.acm.web.model.AgentInstallDTO;
import com.rackspace.salus.acm.web.model.BoundAgentInstallDTO;
import com.rackspace.salus.telemetry.model.AgentType;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.PagedContent;
import com.rackspace.salus.telemetry.model.View;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Pageable;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api")
@Api(value = "Agent installation operations", authorizations = {
    @Authorization(value = "repose_auth",
        scopes = {
            @AuthorizationScope(scope = "write:agent_install", description = "modify Agent Installations in your account"),
            @AuthorizationScope(scope = "read:agent_install", description = "read your Agent Installations"),
            @AuthorizationScope(scope = "delete:agent_install", description = "delete your Agent Installations")
        })
})
public class AgentInstallController implements AgentInstallApi {

  private final AgentInstallRepository agentInstallRepository;
  private final BoundAgentInstallRepository boundAgentInstallRepository;
  private final AgentInstallService agentInstallService;

  @Autowired
  public AgentInstallController(AgentInstallRepository agentInstallRepository,
                                BoundAgentInstallRepository boundAgentInstallRepository,
                                AgentInstallService agentInstallService) {
    this.agentInstallRepository = agentInstallRepository;
    this.boundAgentInstallRepository = boundAgentInstallRepository;
    this.agentInstallService = agentInstallService;
  }

  @Override
  @GetMapping("/admin/bound-agent-installs/{tenantId}/{resourceId}/{agentType}")
  @JsonView(View.Admin.class)
  @ApiOperation(value = "Gets bound agent installation for the given tenant resource and agent type")
  public BoundAgentInstallDTO getBindingForResourceAndAgentType(
      @PathVariable String tenantId, @PathVariable String resourceId,
      @PathVariable AgentType agentType) {
    return boundAgentInstallRepository.findAllByTenantResourceAgentType(tenantId, resourceId, agentType)
        .stream().findFirst()
        .orElseThrow(() -> new NotFoundException("Could find find agent install for given resource and agent type"))
        .toDTO();
  }

  @GetMapping("/tenant/{tenantId}/agent-installs")
  @JsonView(View.Public.class)
  @ApiOperation(value = "Gets all agent installations")
  public PagedContent<AgentInstallDTO> getAgentInstalls(@PathVariable String tenantId,
                                                        Pageable pageable) {
    return PagedContent.fromPage(
        agentInstallRepository.findAllByTenantId(tenantId, pageable)
            .map(AgentInstall::toDTO)
    );
  }

  @PostMapping("/tenant/{tenantId}/agent-installs")
  @ResponseStatus(HttpStatus.CREATED)
  @JsonView(View.Public.class)
  @ApiOperation(value = "Create a new agent installation")
  public AgentInstallDTO create(@PathVariable String tenantId,
                                @RequestBody AgentInstallCreate in) {
    return agentInstallService.install(tenantId, in).toDTO();
  }

  @DeleteMapping("/tenant/{tenantId}/agent-installs/{agentInstallId}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @JsonView(View.Public.class)
  @ApiOperation(value = "Delete an agent installation")
  public void delete(@PathVariable String tenantId, @PathVariable UUID agentInstallId) {
    agentInstallService.delete(tenantId, agentInstallId);
  }
}
