import os
import re
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "orders"
DEFAULT_PROCESSOR_ROLE = "processor_rw"
ROLE_PATTERN = re.compile(r"[a-z_][a-z0-9_]{0,62}")
PROCESSOR_COLUMNS = ("status", "updated_at", "completed_at")

# Los CheckConstraint llevan solo el sufijo: la convencion de Base.metadata antepone ck_<tabla>_.


def _processor_role() -> str:
    role = os.environ.get("PROCESSOR_DB_USER", DEFAULT_PROCESSOR_ROLE).strip()
    if not ROLE_PATTERN.fullmatch(role):
        message = f"PROCESSOR_DB_USER no es un identificador de rol valido: {role!r}"
        raise ValueError(message)
    return role


def upgrade() -> None:
    op.create_table(
        "orders",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("customer_id", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('PENDING', 'COMPLETED')", name="status"),
        sa.PrimaryKeyConstraint("id", name="pk_orders"),
        schema=SCHEMA,
    )
    op.create_index("ix_orders_customer_id", "orders", ["customer_id"], schema=SCHEMA)

    op.create_table(
        "order_items",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(length=80), nullable=False),
        sa.Column("qty", sa.Integer(), nullable=False),
        sa.CheckConstraint("qty >= 1", name="qty_positive"),
        sa.ForeignKeyConstraint(
            ["order_id"],
            [f"{SCHEMA}.orders.id"],
            name="fk_order_items_order_id",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_order_items"),
        schema=SCHEMA,
    )
    op.create_index("ix_order_items_order_id", "order_items", ["order_id"], schema=SCHEMA)

    op.create_table(
        "outbox",
        sa.Column("id", sa.BigInteger(), sa.Identity(), nullable=False),
        sa.Column("event_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=64), nullable=False),
        sa.Column("event_version", sa.SmallInteger(), server_default=sa.text("1"), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("trace_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column(
            "occurred_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column(
            "next_attempt_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id", name="pk_outbox"),
        sa.UniqueConstraint("event_id", name="uq_outbox_event_id"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_outbox_unpublished",
        "outbox",
        ["next_attempt_at"],
        schema=SCHEMA,
        postgresql_where=sa.text("published_at IS NULL"),
    )

    op.create_table(
        "idempotency_keys",
        sa.Column("endpoint", sa.String(length=120), nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("request_hash", sa.CHAR(length=64), nullable=False),
        sa.Column("state", sa.String(length=16), nullable=False),
        sa.Column("order_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("response_status", sa.SmallInteger(), nullable=True),
        sa.Column("response_body", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "expires_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now() + interval '24 hours'"),
            nullable=False,
        ),
        sa.CheckConstraint("state IN ('in_progress', 'completed')", name="state"),
        sa.ForeignKeyConstraint(
            ["order_id"],
            [f"{SCHEMA}.orders.id"],
            name="fk_idempotency_keys_order_id",
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("endpoint", "idempotency_key", name="pk_idempotency_keys"),
        schema=SCHEMA,
    )
    op.create_index(
        "ix_idempotency_keys_expires_at", "idempotency_keys", ["expires_at"], schema=SCHEMA
    )

    role = _processor_role()
    columns = ", ".join(PROCESSOR_COLUMNS)
    op.execute(f'GRANT SELECT ON {SCHEMA}.orders TO "{role}"')
    op.execute(f'GRANT UPDATE ({columns}) ON {SCHEMA}.orders TO "{role}"')


def downgrade() -> None:
    role = _processor_role()
    columns = ", ".join(PROCESSOR_COLUMNS)
    op.execute(f'REVOKE UPDATE ({columns}) ON {SCHEMA}.orders FROM "{role}"')
    op.execute(f'REVOKE SELECT ON {SCHEMA}.orders FROM "{role}"')

    op.drop_index("ix_idempotency_keys_expires_at", table_name="idempotency_keys", schema=SCHEMA)
    op.drop_table("idempotency_keys", schema=SCHEMA)
    op.drop_index("ix_outbox_unpublished", table_name="outbox", schema=SCHEMA)
    op.drop_table("outbox", schema=SCHEMA)
    op.drop_index("ix_order_items_order_id", table_name="order_items", schema=SCHEMA)
    op.drop_table("order_items", schema=SCHEMA)
    op.drop_index("ix_orders_customer_id", table_name="orders", schema=SCHEMA)
    op.drop_table("orders", schema=SCHEMA)
