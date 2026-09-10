SELECT pt.id, pt.user_id, pt.device_id, pt.provider, pt.endpoint,
       pt.p256dh, pt.auth_secret,
       CASE
         WHEN uns.sound_enabled = 0 OR crs.sound_enabled = 0 THEN 0
         ELSE 1
       END AS notification_sound_enabled,
       pt.revision
FROM outbox_jobs AS job
INNER JOIN users AS u ON u.id = job.aggregate_id
INNER JOIN call_device_deliveries AS delivery
  ON delivery.user_id = u.id
 AND delivery.delivery_state = 'ringing'
INNER JOIN call_sessions AS call ON call.id = delivery.call_id
INNER JOIN call_participants AS participant
  ON participant.call_id = call.id
 AND participant.user_id = u.id
 AND participant.participant_state = 'ringing'
LEFT JOIN conversation_members AS member
  ON member.conversation_id = call.conversation_id
 AND member.user_id = u.id
LEFT JOIN user_notification_settings AS uns ON uns.id = u.id
LEFT JOIN call_reception_settings AS crs ON crs.user_id = u.id
LEFT JOIN call_contact_policies AS contact_policy
  ON contact_policy.owner_user_id = u.id
 AND contact_policy.contact_user_id = call.creator_user_id
INNER JOIN push_tokens AS pt
  ON pt.user_id = u.id
 AND pt.device_id = delivery.device_id
INNER JOIN devices AS d ON d.id = pt.device_id
INNER JOIN sessions AS s ON s.device_id = pt.device_id
WHERE job.id = :outbox_job_id
  AND job.event_type = 'call.sync_required'
  AND job.aggregate_type = 'call_user'
  AND job.aggregate_id = delivery.user_id
  AND (
    (call.lifecycle_state = 'ringing' AND call.ringing_deadline_at > :now)
    OR (
      call.lifecycle_state = 'active'
      AND delivery.updated_at > :ringing_started_after
    )
  )
  AND (
    call.kind <> 'conversation_group'
    OR (
      member.left_at IS NULL
      AND (member.muted_until IS NULL OR member.muted_until <= :now)
    )
  )
  AND call.absolute_expires_at > :now
  AND u.disabled_at IS NULL
  AND u.deleted_at IS NULL
  AND (uns.enabled IS NULL OR uns.enabled = 1)
  AND (crs.incoming_calls_enabled IS NULL OR crs.incoming_calls_enabled = 1)
  AND (
    contact_policy.incoming_calls_enabled IS NULL
    OR contact_policy.incoming_calls_enabled = 1
  )
  AND pt.disabled_at IS NULL
  AND d.user_id = pt.user_id
  AND d.revoked_at IS NULL
  AND s.user_id = pt.user_id
  AND s.revoked_at IS NULL
  AND s.expires_at > :now
  AND (s.absolute_expires_at IS NULL OR s.absolute_expires_at > :now)
ORDER BY pt.id;
