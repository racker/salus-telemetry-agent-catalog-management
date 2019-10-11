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
    SELECT   search_labels.agent_install_id
    FROM
      (
        SELECT  inner_ails.agent_install_id,
          COUNT(*) AS count
        FROM     agent_install_label_selectors AS inner_ails
        WHERE    %s
        GROUP BY inner_ails.agent_install_id
      )
      AS     search_labels
    WHERE    search_labels.count >= 1
    GROUP BY search_labels.agent_install_id
  )
)
ORDER BY agent_installs.id