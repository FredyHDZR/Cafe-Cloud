from datetime import timedelta

from sqlalchemy import Interval, literal
from sqlalchemy.sql.elements import ColumnElement


def interval(**parts: float) -> ColumnElement[timedelta]:
    return literal(timedelta(**parts), Interval())
