SELECT pt.id, pt.user_id, pt.device_id, pt.provider, pt.endpoint,
       pt.p256dh, pt.auth_secret,
       m.conversation_id AS notification_conversation_id,
       sender.display_name AS notification_title,
       m.body AS notification_body,
       m.formatting_entities AS notification_entities,
       direct_attachment.original_filename AS direct_attachment_filename,
       forwarded_attachment.original_filename AS forwarded_attachment_filename,
       uns.sound_enabled AS notification_sound_enabled,
       pt.revision
FROM sync_events AS se
INNER JOIN messages AS m ON m.id = se.aggregate_id
INNER JOIN conversations AS conversation ON conversation.id = m.conversation_id
INNER JOIN users AS u ON u.id = se.user_id
INNER JOIN users AS sender ON sender.id = m.sender_user_id
INNER JOIN conversation_members AS cm
  ON cm.conversation_id = m.conversation_id AND cm.user_id = se.user_id
LEFT JOIN message_attachments AS ma
  ON ma.message_id = m.id AND ma.position = 0
LEFT JOIN attachments AS direct_attachment
  ON direct_attachment.id = ma.attachment_id
LEFT JOIN forwarded_message_attachments AS fma
  ON fma.message_id = m.id AND fma.position = 0
LEFT JOIN attachments AS forwarded_attachment
  ON forwarded_attachment.id = fma.attachment_id
LEFT JOIN user_notification_settings AS uns ON uns.id = se.user_id
INNER JOIN push_tokens AS pt ON pt.user_id = se.user_id
INNER JOIN devices AS d ON d.id = pt.device_id
INNER JOIN sessions AS s ON s.device_id = pt.device_id
WHERE se.outbox_job_id = :outbox_job_id
  AND se.event_type = 'message.created'
  AND se.aggregate_type = 'message'
  AND se.user_id <> m.sender_user_id
  AND conversation.self_owner_id IS NULL
  AND u.disabled_at IS NULL
  AND u.deleted_at IS NULL
  AND cm.left_at IS NULL
  AND (cm.muted_until IS NULL OR cm.muted_until <= :now)
  AND (uns.enabled IS NULL OR uns.enabled = 1)
  AND pt.disabled_at IS NULL
  AND d.user_id = pt.user_id
  AND d.revoked_at IS NULL
  AND s.user_id = pt.user_id
  AND s.revoked_at IS NULL
  AND s.expires_at > :now
  AND (s.absolute_expires_at IS NULL OR s.absolute_expires_at > :now)
ORDER BY pt.id;
