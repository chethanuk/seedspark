# -*- coding: utf-8 -*-
import numbers
import re
from datetime import datetime
from typing import Union

_unit_in_ms_without_week = {"s": 1000, "m": 60000, "h": 3600000, "d": 86400000}
_unit_in_ms = {**_unit_in_ms_without_week, "w": 604800000}


def convert_datetime_to_ms(dt: datetime) -> int:
    """
    convert_datetime_to_ms Convert datetime to millis

    Convert datetime to millis _format in int representation

    Parameters
    ----------
    dt : datetime
        datetime object which needs to be converted to milliseconds

    Returns
    -------
    int
        Milliseconds representation of datetime object
    """
    epoch = datetime.utcfromtimestamp(0)
    return int((dt - epoch).total_seconds() * 1000.0)


def convert_ms_to_datetime(ms: Union[int, float]) -> datetime:
    """
    ms_to_datetime [summary]

    [extended_summary]

    Parameters
    ----------
    ms : Union[int, float]
        [description]

    Returns
    -------
    datetime
        [description]
    """
    if ms < 0:
        raise ValueError("ms must be greater than or equal to zero.")
    return datetime.utcfromtimestamp(ms / 1000)


def time_string_to_ms(pattern: str, time_string: str, unit_in_ms):
    pattern = pattern.format("|".join(unit_in_ms))
    res = re.fullmatch(pattern, time_string)
    if res:
        magnitude = int(res.group(1))
        return magnitude * unit_in_ms[res.group(2)]
    return None


def time_ago_to_ms(self, time_ago_string: str) -> int:
    """Returns millisecond representation of time-ago string"""
    if time_ago_string == "now":
        return 0
    ms = time_string_to_ms(r"(\d+)({})-ago", time_ago_string, self._unit_in_ms)
    if ms is None:
        raise ValueError(
            f"Invalid time-ago _format: `{time_ago_string}`. Must be on _format <integer>(s|m|h|d|w)-ago "
            f"or 'now'. E.g. '3d-ago' or '1w-ago'."
        )
    return ms


def convert_timestamp_to_ms(timestamp: Union[datetime, str, int, float]) -> int:
    """
    convert_timestamp_to_ms Converts timestamp _format to milliseconds since epoch

    Convert the any timestamp given in milliseconds or time-ago _format or dattime object
    to Milliseconds since epoch representation of timestamp

    Parameters
    ----------
    timestamp : Union[datetime, str, int, float]
        Milliseconds since epoch

    Returns
    -------
    int
        Milliseconds since epoch representation of timestamp
    """
    if isinstance(timestamp, numbers.Number):  # float, int, int64 etc
        return int(timestamp)

    if isinstance(timestamp, datetime):
        ms = convert_datetime_to_ms(timestamp)
    else:
        raise TypeError(
            f"Timestamp `{timestamp}` was of type {type(timestamp)}, "
            "but must be int, float, str or datetime"
        )

    if ms < 0:
        raise ValueError(
            f"Timestamps can't be negative - "
            f"they must represent a time after 1.1.1970, but {ms} was provided"
        )

    return ms
