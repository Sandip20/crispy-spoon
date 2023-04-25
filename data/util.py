"""
Utility Module 
"""
import pandas as pd

def data_frame_to_dict(data_frame):
    """
    Convert a pandas DataFrame to a list of dictionaries.

    Args:
        df (pandas.DataFrame): The DataFrame to convert.

    Returns:
        A list of dictionaries, where each dictionary represents a row in the DataFrame.
        The dictionary keys are the column names and the values are the corresponding row values.

    Example:
        >>> df = pd.DataFrame({'A': [1, 2, 3], 'B': ['foo', 'bar', 'baz']})
        >>> data_frame_to_dict(df)
        [{'A': 1, 'B': 'foo'}, {'A': 2, 'B': 'bar'}, {'A': 3, 'B': 'baz'}]
    """
    data_frame['Date'] = pd.to_datetime(data_frame.index)
    data_frame['Expiry'] = pd.to_datetime(data_frame['Expiry'])
    return data_frame.to_dict('records')
