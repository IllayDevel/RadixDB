SELECT member.id, member.conversation_id, member.user_id, member.role,
       member.joined_seq, member.history_cleared_through_seq,
       member.last_delivered_seq, member.last_read_seq, member.joined_at,
       member.left_at, member.muted_until, member.revision
FROM conversation_members AS own
INNER JOIN conversation_members AS member
  ON member.conversation_id = own.conversation_id
WHERE own.user_id = :user_id
  AND own.left_at IS NULL
ORDER BY member.conversation_id, member.id;
