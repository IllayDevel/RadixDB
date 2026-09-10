SELECT member.user_id, user.username, user.display_name, user.account_kind,
       user.avatar_attachment_id, user.profile_revision
FROM conversation_members AS own
INNER JOIN conversation_members AS member
  ON member.conversation_id = own.conversation_id
INNER JOIN users AS user ON user.id = member.user_id
WHERE own.user_id = :user_id
  AND own.left_at IS NULL
  AND member.left_at IS NULL
  AND user.disabled_at IS NULL
  AND user.deleted_at IS NULL
ORDER BY member.user_id;
