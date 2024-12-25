# utils.py

from datetime import datetime, date
from typing import List, Any

def parse_trade_date(date_input: Any, fmt: str) -> date:
    """
    Parses the trade date.

    This function handles both string inputs and `datetime.date` objects.
    If the input is a string, it parses it using the provided format.
    If the input is already a `datetime.date` object, it returns it unchanged.
    Raises a `ValueError` for unsupported types or incorrect formats.

    Args:
        date_input (str or datetime.date): The trade date input.
        fmt (str): The expected date format (e.g., "%Y-%m-%d").

    Returns:
        datetime.date: The parsed trade date.

    Raises:
        ValueError: If the input type is unsupported or the format is incorrect.
    """
    if isinstance(date_input, str):
        try:
            parsed_date = datetime.strptime(date_input, fmt).date()
            return parsed_date
        except ValueError as ve:
            raise ValueError(f"Error parsing date string '{date_input}': {ve}")
    elif isinstance(date_input, date):
        return date_input
    else:
        raise ValueError(
            f"Unsupported type for trade_date: {type(date_input).__name__}. "
            f"Expected 'str' or 'datetime.date'."
        )

def chunk_list(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """
    Splits a list into smaller chunks of specified size.

    This utility function is useful for processing large datasets in manageable batches.

    Args:
        data (List[Any]): The list to split into chunks.
        chunk_size (int): The maximum size of each chunk.

    Returns:
        List[List[Any]]: A list containing smaller lists (chunks), each with up to `chunk_size` elements.

    Example:
        >>> chunk_list([1, 2, 3, 4, 5], 2)
        [[1, 2], [3, 4], [5]]
    """
    if chunk_size <= 0:
        raise ValueError("chunk_size must be a positive integer.")
    
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]
