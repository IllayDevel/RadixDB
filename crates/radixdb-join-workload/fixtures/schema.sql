CREATE TABLE attachments (
    id UUID PRIMARY KEY,
    original_filename TEXT NOT NULL
);

CREATE TABLE users (
    id UUID PRIMARY KEY,
    username TEXT NOT NULL,
    display_name TEXT NOT NULL,
    account_kind TEXT NOT NULL,
    avatar_attachment_id UUID,
    profile_revision INTEGER NOT NULL,
    disabled_at TIMESTAMP,
    deleted_at TIMESTAMP
);

CREATE TABLE devices (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    last_seen_at TIMESTAMP NOT NULL,
    revoked_at TIMESTAMP
);

CREATE TABLE sessions (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    device_id UUID NOT NULL REFERENCES devices(id),
    expires_at TIMESTAMP NOT NULL,
    absolute_expires_at TIMESTAMP,
    revoked_at TIMESTAMP
);

CREATE TABLE conversations (
    id UUID PRIMARY KEY,
    self_owner_id UUID REFERENCES users(id)
);

CREATE TABLE conversation_members (
    id UUID PRIMARY KEY,
    conversation_id UUID NOT NULL REFERENCES conversations(id),
    user_id UUID NOT NULL REFERENCES users(id),
    role TEXT NOT NULL,
    joined_seq INTEGER NOT NULL,
    history_cleared_through_seq INTEGER NOT NULL,
    last_delivered_seq INTEGER NOT NULL,
    last_read_seq INTEGER NOT NULL,
    joined_at TIMESTAMP NOT NULL,
    left_at TIMESTAMP,
    muted_until TIMESTAMP,
    revision INTEGER NOT NULL
);

CREATE TABLE messages (
    id UUID PRIMARY KEY,
    conversation_id UUID NOT NULL REFERENCES conversations(id),
    sender_user_id UUID NOT NULL REFERENCES users(id),
    body TEXT NOT NULL,
    formatting_entities TEXT NOT NULL
);

CREATE TABLE outbox_jobs (
    id UUID PRIMARY KEY,
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID,
    state TEXT NOT NULL,
    available_at TIMESTAMP NOT NULL,
    lease_until TIMESTAMP,
    revision INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL
);

CREATE TABLE sync_events (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    event_type TEXT NOT NULL,
    aggregate_type TEXT NOT NULL,
    aggregate_id UUID,
    outbox_job_id UUID NOT NULL REFERENCES outbox_jobs(id)
);

CREATE TABLE message_attachments (
    message_id UUID NOT NULL REFERENCES messages(id),
    attachment_id UUID PRIMARY KEY REFERENCES attachments(id),
    position INTEGER NOT NULL
);

CREATE TABLE forwarded_message_attachments (
    id UUID PRIMARY KEY,
    message_id UUID NOT NULL REFERENCES messages(id),
    attachment_id UUID NOT NULL REFERENCES attachments(id),
    position INTEGER NOT NULL
);

CREATE TABLE user_notification_settings (
    id UUID PRIMARY KEY REFERENCES users(id),
    enabled INTEGER NOT NULL,
    sound_enabled INTEGER NOT NULL,
    stream_notifications_enabled INTEGER NOT NULL
);

CREATE TABLE push_tokens (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES users(id),
    device_id UUID NOT NULL REFERENCES devices(id),
    provider TEXT NOT NULL,
    endpoint TEXT NOT NULL,
    p256dh TEXT,
    auth_secret TEXT,
    disabled_at TIMESTAMP,
    revision INTEGER NOT NULL
);

CREATE TABLE call_reception_settings (
    user_id UUID PRIMARY KEY REFERENCES users(id),
    incoming_calls_enabled INTEGER NOT NULL,
    sound_enabled INTEGER NOT NULL
);

CREATE TABLE call_contact_policies (
    id UUID PRIMARY KEY,
    owner_user_id UUID NOT NULL REFERENCES users(id),
    contact_user_id UUID NOT NULL REFERENCES users(id),
    incoming_calls_enabled INTEGER NOT NULL
);

CREATE TABLE call_sessions (
    id UUID PRIMARY KEY,
    kind TEXT NOT NULL,
    conversation_id UUID REFERENCES conversations(id),
    creator_user_id UUID NOT NULL REFERENCES users(id),
    lifecycle_state TEXT NOT NULL,
    ringing_deadline_at TIMESTAMP,
    absolute_expires_at TIMESTAMP NOT NULL
);

CREATE TABLE call_participants (
    id UUID PRIMARY KEY,
    call_id UUID NOT NULL REFERENCES call_sessions(id),
    user_id UUID NOT NULL REFERENCES users(id),
    participant_state TEXT NOT NULL
);

CREATE TABLE call_device_deliveries (
    id UUID PRIMARY KEY,
    call_id UUID NOT NULL REFERENCES call_sessions(id),
    user_id UUID NOT NULL REFERENCES users(id),
    device_id UUID NOT NULL REFERENCES devices(id),
    delivery_state TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE TABLE stream_upstreams (
    id UUID PRIMARY KEY,
    title TEXT
);

CREATE TABLE streams (
    id UUID PRIMARY KEY,
    owner_user_id UUID NOT NULL REFERENCES users(id),
    upstream_id UUID NOT NULL REFERENCES stream_upstreams(id),
    state TEXT NOT NULL,
    notifications_enabled INTEGER NOT NULL,
    deleted_at TIMESTAMP
);

CREATE TABLE stream_publications (
    id UUID PRIMARY KEY,
    upstream_id UUID NOT NULL REFERENCES stream_upstreams(id),
    sort_key INTEGER NOT NULL,
    text_content TEXT,
    deleted_at TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
);

CREATE TABLE stream_read_states (
    id UUID PRIMARY KEY,
    stream_id UUID NOT NULL REFERENCES streams(id),
    owner_user_id UUID NOT NULL REFERENCES users(id),
    last_read_sort_key INTEGER NOT NULL,
    last_read_publication_id UUID REFERENCES stream_publications(id)
);

CREATE TABLE stream_publication_push_deliveries (
    publication_id UUID NOT NULL REFERENCES stream_publications(id),
    stream_id UUID NOT NULL REFERENCES streams(id),
    owner_user_id UUID NOT NULL REFERENCES users(id),
    outbox_job_id UUID NOT NULL REFERENCES outbox_jobs(id)
);

CREATE UNIQUE INDEX conversation_members_scope_uidx
    ON conversation_members (conversation_id, user_id);
CREATE INDEX conversation_members_user_idx
    ON conversation_members (user_id, left_at, conversation_id);
CREATE INDEX devices_user_last_seen_idx ON devices (user_id, last_seen_at);
CREATE INDEX sessions_user_device_idx
    ON sessions (user_id, device_id, revoked_at);
CREATE UNIQUE INDEX sync_events_outbox_user_uidx
    ON sync_events (outbox_job_id, user_id);
CREATE UNIQUE INDEX message_attachments_message_position_uidx
    ON message_attachments (message_id, position);
CREATE UNIQUE INDEX forwarded_message_attachments_position_uidx
    ON forwarded_message_attachments (message_id, position);
CREATE INDEX push_tokens_user_state_idx
    ON push_tokens (user_id, disabled_at);
CREATE UNIQUE INDEX push_tokens_device_provider_uidx
    ON push_tokens (device_id, provider);
CREATE INDEX outbox_jobs_available_idx
    ON outbox_jobs (state, available_at);
CREATE UNIQUE INDEX call_contact_policies_owner_contact_uidx
    ON call_contact_policies (owner_user_id, contact_user_id);
CREATE UNIQUE INDEX call_participants_call_user_uidx
    ON call_participants (call_id, user_id);
CREATE INDEX call_device_deliveries_user_state_idx
    ON call_device_deliveries (user_id, delivery_state, updated_at, call_id);
CREATE UNIQUE INDEX streams_owner_upstream_uidx
    ON streams (owner_user_id, upstream_id);
CREATE INDEX stream_publications_order_idx
    ON stream_publications (upstream_id, sort_key, id);
CREATE UNIQUE INDEX stream_read_states_stream_uidx
    ON stream_read_states (stream_id, owner_user_id);
CREATE UNIQUE INDEX stream_publication_push_deliveries_uidx
    ON stream_publication_push_deliveries (publication_id, stream_id);
CREATE INDEX stream_publication_push_deliveries_outbox_idx
    ON stream_publication_push_deliveries (outbox_job_id, stream_id);
