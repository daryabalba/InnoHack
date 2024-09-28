import pandas as pd
import numpy as np
import re


def edit_year(x):
    if len(x) == 4:
        return x if 2024 > int(x) > 1930 else None

    if len(x) == 3:
        x = "".join(["1", x])
        return x if 2024 > int(x) > 1930 else None
    elif len(x) == 2:
        x = "".join(["19", x])
        return x if 2024 > int(x) > 1930 else None
    else:
        return None


def edit_name(x):
    x = re.sub("[^a-яё ]+", "", x.lower()).strip()
    return x if len(x) > 2 else ""


def delete_nums(number):
    number = re.sub("[^\d]", "", number)
    return number if (len(number) <= 11) and (len(number) >= 7) else np.nan


def extract_regcode(number):
    number = str(number)

    if len(number) == 7:
        return np.nan, number

    if len(number) == 8:
        return np.nan, number[1:]

    if len(number) == 11:
        number = number[1:]

    return number[:-7], number[-7:]


def phone_preprocessing(number):
    clean_number = delete_nums(number)
    region_code, num = extract_regcode(clean_number)
    return region_code, num


def process_email_custom(email):
    clean_pattern = re.compile(r"[^a-zA-Z0-9_]")
    tlds = ["net", "org", "ru", "com"]

    if "@" in email:
        login, domain = email.split("@", 1)
        login = clean_pattern.sub("", login)
        domain_parts = re.split(r"[./,]", domain)
        second_level_domain = (
            domain_parts[0] if len(domain_parts) > 1 else domain_parts[0]
        )

        for tld in tlds:
            if second_level_domain.endswith(tld):
                second_level_domain = second_level_domain[: -len(tld)]
                break

        return login, second_level_domain
    else:
        cleaned_email = clean_pattern.sub("", email)
        match = re.search(r"\d+", cleaned_email[::-1])
        if match:
            end_of_login = len(cleaned_email) - match.start()
            login = cleaned_email[:end_of_login]
            domain = (
                cleaned_email[end_of_login:]
                if end_of_login < len(cleaned_email)
                else None
            )
            if domain:
                domain_parts = re.split(r"[./,]", domain)
                second_level_domain = (
                    domain_parts[0] if len(domain_parts) > 1 else domain_parts[0]
                )
                for tld in tlds:
                    if second_level_domain.endswith(tld):
                        second_level_domain = second_level_domain[: -len(tld)]
                        break
            else:
                second_level_domain = None
        else:
            login = cleaned_email
            second_level_domain = None

        return login, second_level_domain


def get_index(cell):
    idx = re.findall(r"\d{6}", cell)
    if idx:
        return idx[0]
    return None


cities = [
    "г.",
    "с.",
    "клх",
    "к.",
    "д.",
    "п.",
    "ст.",
    "Село",
    "Деревня",
    "Поселок",
    "Посёлок",
    "Город",
    "Д.*р.*в.*н.",
    "П.*с.*л.*к",
    "С.*л.*",
    ".*ело",
    "с.*л.*",
]
streets = [
    "ул.",
    "у.*.",
    "пер.",
    "бул.",
    "пр.",
    "ш.",
    "алл.",
    "наб.",
    "Улица",
    "Шоссе",
    ".*аб",
    "н.б.",
    "бу.*.",
    "б.*л.",
    ".*л.*ц.*",
]
house = ["д.", "Дом", ".*ом"]
building = ["Строение", "к.", "стр.", ".*тр.", ".*тр.ение", ".*в.*р.*ира"]


def find_city(text):
    """
    Находит название города после сокращения.

    Args:
        text: Строка, в которой нужно найти город.

    Returns:
        Название города, если оно найдено.
    """
    pattern = r"(?:" + "|".join(cities) + r")\s+(\D+?)(?=\s*\(\w+\)|,\s*[\w\./]+)"
    match = re.search(pattern, text)
    if match:
        return match.group(1)
    return None


def find_streets(text):
    """
    Находит название города после сокращения.

    Args:
        text: Строка, в которой нужно найти город.

    Returns:
        Название города, если оно найдено.
    """
    pattern = r"(?:" + "|".join(streets) + r")\s+(.+?)(?=\n|,\s*[\w\./]+)"
    match = re.search(pattern, text)
    if match:
        return match.group(1)
    return None


def find_house_and_building(text):
    """Находит номер дома и строения в строке."""
    house_pattern = r"(?:" + "|".join(house) + r")\s+(\d+[\/\d]*)"
    building_pattern = r"(?:" + "|".join(building) + r")\s+(\d+[\/\d\w]*)"

    house_match = re.search(house_pattern, text)
    building_match = re.search(building_pattern, text)

    if house_match:
        house_number = house_match.group(1)
    else:
        house_number = None

    if building_match:
        building_number = building_match.group(1)
    else:
        building_number = None

    if house_number and building_number:
        return f"{house_number}_{building_number}"
    elif house_number:
        return house_number
    else:
        return None


def clean_adress(dataset):
    dataset['address'] = dataset['address'].apply(lambda x: x.replace('\n', ''))
    dataset['address_index'] = dataset['address'].apply(get_index)
    dataset['city'] = dataset['address'].apply(find_city)
    dataset['street'] = dataset['address'].apply(find_streets)
    dataset['house_and_building'] = dataset['address'].apply(find_house_and_building)