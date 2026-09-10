from tests.support import Warehouse

PROCESSOR_ROLE = "processor_rw"
UPDATABLE_COLUMNS = {"status", "completed_at", "updated_at"}


def test_the_processor_role_only_updates_the_three_columns_of_adr_006(
    warehouse: Warehouse,
) -> None:
    granted = warehouse.column_grants(
        role=PROCESSOR_ROLE, schema="orders", table="orders", privilege="UPDATE"
    )

    assert granted == UPDATABLE_COLUMNS


def test_the_processor_role_can_read_the_order_it_has_to_complete(warehouse: Warehouse) -> None:
    granted = warehouse.column_grants(
        role=PROCESSOR_ROLE, schema="orders", table="orders", privilege="SELECT"
    )

    assert {"id", "status", "completed_at"} <= granted
