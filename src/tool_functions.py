import pandas as pd
import numpy as np
import re


def edit_year(x):
    if len(x) == 4:
        return x if 2024 > int(x) > 1930 else None

    if len(x) == 3:
        x = ''.join(['1', x])
        return x if 2024 > int(x) > 1930 else None
    elif len(x) == 2:
        x = ''.join(['19', x])
        return x if 2024 > int(x) > 1930 else None
    else:
        return None


def edit_name(x):
    x = re.sub('[^a-яё ]+', '', x.lower()).strip()
    return x if len(x) > 2 else ''


def delete_nums(number):
    number = re.sub('[^\d]', '', number)
    return number if (len(number) <= 11) and (len(number) >= 7) else np.nan
