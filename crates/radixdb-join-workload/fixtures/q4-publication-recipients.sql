SELECT s.id AS stream_id, s.owner_user_id AS owner_user_id
FROM streams AS s
INNER JOIN users AS u ON u.id = s.owner_user_id
INNER JOIN stream_read_states AS read_state
  ON read_state.stream_id = s.id AND read_state.owner_user_id = s.owner_user_id
LEFT JOIN user_notification_settings AS settings ON settings.id = s.owner_user_id
WHERE s.upstream_id = :upstream_id
  AND s.state = 'active'
  AND s.deleted_at IS NULL
  AND s.notifications_enabled = 1
  AND u.disabled_at IS NULL
  AND u.deleted_at IS NULL
  AND (settings.enabled IS NULL OR settings.enabled = 1)
  AND (
    settings.stream_notifications_enabled IS NULL
    OR settings.stream_notifications_enabled = 1
  )
  AND (
    read_state.last_read_sort_key < :sort_key
    OR (
      read_state.last_read_sort_key = :sort_key
      AND (
        read_state.last_read_publication_id IS NULL
        OR read_state.last_read_publication_id < :publication_id
      )
    )
  )
ORDER BY s.id
LIMIT :recipient_limit;
