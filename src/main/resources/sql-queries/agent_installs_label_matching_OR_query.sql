SELECT agent_installs.id
FROM   agent_installs JOIN agent_install_label_selectors AS ail
WHERE  agent_installs.id = ail.agent_install_id
AND agent_installs.label_selector_method = 'OR'
AND agent_installs.id IN
(
  SELECT ails.agent_install_id
  FROM   agent_install_label_selectors AS ails
  WHERE  agent_installs.id IN
  (
    SELECT ai.id
    FROM   agent_installs AS ai
    WHERE  ai.tenant_id = :tenantId
  )
  AND    agent_installs.id IN
  (
    SELECT  inner_ails.agent_install_id
    FROM     agent_install_label_selectors AS inner_ails
    WHERE    %s
    GROUP BY inner_ails.agent_install_id
    HAVING COUNT(*) >= 1
  )
)
ORDER BY agent_installs.id