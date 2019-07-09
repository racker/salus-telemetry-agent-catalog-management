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
import com.rackspace.salus.acm.entities.AgentRelease;
import com.rackspace.salus.acm.repositories.AgentReleaseRepository;
import com.rackspace.salus.acm.services.AgentReleaseService;
import com.rackspace.salus.acm.web.model.AgentReleaseCreate;
import com.rackspace.salus.acm.web.model.AgentReleaseDTO;
import com.rackspace.salus.telemetry.model.NotFoundException;
import com.rackspace.salus.telemetry.model.PagedContent;
import com.rackspace.salus.telemetry.model.View;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.Authorization;
import io.swagger.annotations.AuthorizationScope;
import java.util.UUID;
import javax.validation.Valid;
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
@Api(value = "Agent release operations", authorizations = {
    @Authorization(value = "repose_auth",
        scopes = {
            @AuthorizationScope(scope = "write:agent_release", description = "modify Agent Releases"),
            @AuthorizationScope(scope = "read:agent_release", description = "read your Agent Releases"),
            @AuthorizationScope(scope = "delete:agent_release", description = "delete your Agent Releases")
        })
})
public class AgentReleaseController {

  private final AgentReleaseRepository agentReleaseRepository;
  private final AgentReleaseService agentReleaseService;

  @Autowired
  public AgentReleaseController(AgentReleaseRepository agentReleaseRepository,
                                AgentReleaseService agentReleaseService) {
    this.agentReleaseRepository = agentReleaseRepository;
    this.agentReleaseService = agentReleaseService;
  }

  @GetMapping("/tenant/{tenantId}/agent-releases")
  @JsonView(View.Public.class)
  @ApiOperation(value = "Get available agent releases")
  public PagedContent<AgentReleaseDTO> getAgentReleasesForTenant(@PathVariable String tenantId,
                                                                 Pageable pageable) {
    // tenantId isn't actually used, but it present to keep a consitent request path structure
    // across other APIs

    return PagedContent.fromPage(
        agentReleaseRepository.findAll(pageable)
            .map(AgentRelease::toDTO)
    );

  }

  @GetMapping("/tenant/{tenantId}/agent-releases/{agentReleaseId}")
  @JsonView(View.Public.class)
  @ApiOperation(value = "Get a specific agent release")
  public AgentReleaseDTO getAgentReleaseForTenant(@PathVariable String tenantId,
                                                                 @PathVariable UUID agentReleaseId,
                                                                 Pageable pageable) {
    // tenantId isn't actually used, but it present to keep a consitent request path structure
    // across other APIs

    return agentReleaseRepository.findById(agentReleaseId)
        .orElseThrow(() -> new NotFoundException("Unable to find agent release"))
        .toDTO();
  }

  @GetMapping("/admin/agent-releases")
  @JsonView(View.Admin.class)
  @ApiOperation(value = "Get available agent releases")
  public PagedContent<AgentReleaseDTO> getAgentReleases(Pageable pageable) {

    return PagedContent.fromPage(
        agentReleaseRepository.findAll(pageable)
            .map(AgentRelease::toDTO)
    );

  }

  @GetMapping("/admin/agent-releases/{agentReleaseId}")
  @JsonView(View.Admin.class)
  @ApiOperation(value = "Get a specific agent release")
  public AgentReleaseDTO getAgentRelease(@PathVariable UUID agentReleaseId) {

    return agentReleaseRepository.findById(agentReleaseId)
        .orElseThrow(() -> new NotFoundException("Unable to find agent release"))
        .toDTO();
  }

  @PostMapping("/admin/agent-releases")
  @ResponseStatus(HttpStatus.CREATED)
  @JsonView(View.Admin.class)
  @ApiOperation(value = "Declare a new agent release")
  public AgentReleaseDTO declareAgentRelease(@RequestBody @Valid AgentReleaseCreate in) {
    return agentReleaseService.create(in);
  }

  @DeleteMapping("/admin/agent-releases/{agentReleaseId}")
  @ResponseStatus(HttpStatus.NO_CONTENT)
  @JsonView(View.Admin.class)
  @ApiOperation(value = "Delete an agent release")
  public void delete(@PathVariable UUID agentReleaseId) {
    agentReleaseService.delete(agentReleaseId);
  }
}
