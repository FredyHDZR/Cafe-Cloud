from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

SCHEMA = "orders"
INDEX = "ix_outbox_unpublished"
PENDING_WHERE = "published_at IS NULL AND failed_at IS NULL"
PREVIOUS_WHERE = "published_at IS NULL"


def upgrade() -> None:
    op.add_column(
        "outbox",
        sa.Column("failed_at", sa.DateTime(timezone=True), nullable=True),
        schema=SCHEMA,
    )
    op.drop_index(INDEX, table_name="outbox", schema=SCHEMA)
    # Una fila agotada sale del indice de trabajo: el publicador no vuelve a leerla cada ciclo.
    op.create_index(
        INDEX,
        "outbox",
        ["next_attempt_at"],
        schema=SCHEMA,
        postgresql_where=sa.text(PENDING_WHERE),
    )


def downgrade() -> None:
    op.drop_index(INDEX, table_name="outbox", schema=SCHEMA)
    op.create_index(
        INDEX,
        "outbox",
        ["next_attempt_at"],
        schema=SCHEMA,
        postgresql_where=sa.text(PREVIOUS_WHERE),
    )
    op.drop_column("outbox", "failed_at", schema=SCHEMA)
