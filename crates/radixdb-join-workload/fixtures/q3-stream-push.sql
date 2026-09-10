SELECT pt.id, pt.user_id, pt.device_id, pt.provider, pt.endpoint,
       pt.p256dh, pt.auth_secret,
       delivery.stream_id AS notification_stream_id,
       delivery.publication_id AS notification_publication_id,
       CASE
         WHEN upstream.title IS NULL OR upstream.title = '' THEN 'Поток'
         ELSE upstream.title
       END AS notification_title,
       CASE
         WHEN publication.text_content IS NULL OR publication.text_content = ''
           THEN 'Новая публикация'
         ELSE publication.text_content
       END AS notification_body,
       CASE WHEN settings.sound_enabled = 0 THEN 0 ELSE 1 END
         AS notification_sound_enabled,
       pt.revision
FROM outbox_jobs AS job
INNER JOIN stream_publication_push_deliveries AS delivery
  ON delivery.outbox_job_id = job.id
INNER JOIN stream_publications AS publication
  ON publication.id = delivery.publication_id
INNER JOIN stream_upstreams AS upstream ON upstream.id = publication.upstream_id
INNER JOIN streams AS owned_stream
  ON owned_stream.id = delivery.stream_id
 AND owned_stream.owner_user_id = delivery.owner_user_id
 AND owned_stream.upstream_id = publication.upstream_id
INNER JOIN stream_read_states AS read_state
  ON read_state.stream_id = owned_stream.id
 AND read_state.owner_user_id = delivery.owner_user_id
INNER JOIN users AS owner_user ON owner_user.id = delivery.owner_user_id
LEFT JOIN user_notification_settings AS settings
  ON settings.id = delivery.owner_user_id
INNER JOIN push_tokens AS pt ON pt.user_id = delivery.owner_user_id
INNER JOIN devices AS device ON device.id = pt.device_id
INNER JOIN sessions AS session ON session.device_id = pt.device_id
WHERE job.id = :outbox_job_id
  AND job.event_type = 'stream.publication_created'
  AND job.aggregate_type = 'stream_publication'
  AND job.aggregate_id = publication.id
  AND owned_stream.state = 'active'
  AND owned_stream.deleted_at IS NULL
  AND owned_stream.notifications_enabled = 1
  AND publication.deleted_at IS NULL
  AND publication.expires_at > :now
  AND owner_user.disabled_at IS NULL
  AND owner_user.deleted_at IS NULL
  AND (settings.enabled IS NULL OR settings.enabled = 1)
  AND (
    settings.stream_notifications_enabled IS NULL
    OR settings.stream_notifications_enabled = 1
  )
  AND (
    read_state.last_read_sort_key < publication.sort_key
    OR (
      read_state.last_read_sort_key = publication.sort_key
      AND (
        read_state.last_read_publication_id IS NULL
        OR read_state.last_read_publication_id < publication.id
      )
    )
  )
  AND pt.disabled_at IS NULL
  AND device.user_id = pt.user_id
  AND device.revoked_at IS NULL
  AND session.user_id = pt.user_id
  AND session.revoked_at IS NULL
  AND session.expires_at > :now
  AND (session.absolute_expires_at IS NULL OR session.absolute_expires_at > :now)
ORDER BY pt.id;
