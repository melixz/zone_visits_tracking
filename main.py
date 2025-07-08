#!/usr/bin/env python3

import logging
import sys
from enum import StrEnum, auto

import click
import pandas as pd
import requests
from rich.console import Console
from rich.logging import RichHandler
from rich.table import Table

logging.basicConfig(
    level=logging.INFO,
    format="%(levelname)s - %(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)


class Metric(StrEnum):
    ENTRIES = auto()
    EXITS = auto()


def load_reference_data(file_path: str) -> pd.DataFrame:
    """Загружает справочник зон из Excel-файла."""
    logging.info(f"Загрузка справочника из {file_path}")
    try:
        df = pd.read_excel(file_path)
        assert "GUID" in df.columns and "Наименование" in df.columns, (
            "Справочник должен содержать столбцы 'GUID' и 'Наименование'"
        )
        return df
    except Exception as e:
        logging.critical(f"Ошибка загрузки справочника: {e}")
        sys.exit(1)


def fetch_visitors_data(api_host: str, date: str, guids: list[str]) -> dict:
    """Запрашивает данные о посещениях с сервера."""
    logging.info(f"Запрос данных о посещениях с {api_host} на дату {date}")
    try:
        base_url = f"http://{api_host}/borders/chart-data"
        params = {
            "mode": "halfhour",
            "date": date,
            "objects": ",".join(guids),
        }
        response = requests.get(base_url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        assert "data" in data and isinstance(data["data"], list), (
            "Ответ API должен содержать ключ 'data' с массивом значений"
        )
        if not data["data"]:
            logging.critical("Сервер вернул пустой список 'data'")
            sys.exit(1)
        return data
    except Exception as e:
        logging.critical(f"Ошибка при запросе API: {e}")
        sys.exit(1)


def create_visitors_table(
    reference: pd.DataFrame, visitors: dict, metric: Metric
) -> Table:
    """Создает таблицу с данными о посещениях по зонам и времени."""
    table = Table(title=f"Посещения зон ({metric.value}) за {visitors['picker_date']}")
    table.add_column("Время", style="cyan", justify="center")

    guid_to_name = dict(zip(reference["GUID"], reference["Наименование"]))
    zone_guids = visitors.get("selected", [])

    if not zone_guids:
        logging.critical("Список 'selected' пуст. Нет выбранных зон.")
        sys.exit(1)

    for guid in zone_guids:
        name = guid_to_name.get(guid, guid)
        table.add_column(name, style="magenta", justify="center")

    guid_data_map = {
        zone[0]: zone[3] for zone in visitors["data"] if zone[0] in zone_guids
    }

    time_slots_set = set()
    for time_slots in guid_data_map.values():
        for slot in time_slots:
            time_slots_set.add(slot[0])
    sorted_times = sorted(time_slots_set)

    for time in sorted_times:
        row_data = [time]
        for guid in zone_guids:
            value = "0"
            for slot in guid_data_map.get(guid, []):
                if slot[0] == time:
                    value = str(slot[1]) if metric == Metric.ENTRIES else str(slot[2])
                    break
            row_data.append(f"[dim]{value}[/]" if value == "0" else value)
        table.add_row(*row_data)

    return table


@click.command()
@click.option(
    "--metric",
    default=Metric.ENTRIES,
    type=click.Choice(Metric),
    help="Метрика для отображения: entries (входы) или exits (выходы)",
)
@click.option(
    "--reference-file",
    default="Справочник.xlsx",
    type=click.Path(exists=True, dir_okay=False),
    help="Путь к файлу справочника зон (.xlsx)",
)
@click.option(
    "--api-host",
    required=True,
    help="IP:port или hostname API-сервера (например 127.0.0.1:9006)",
)
@click.option(
    "--date",
    required=True,
    help="Дата отчета в формате ДД.ММ.ГГГГ (например 12.06.2020)",
)
def main(metric: Metric, reference_file: str, api_host: str, date: str):
    """Создает таблицу посещений зон за указанную дату с интервалом 30 минут."""
    console = Console()

    reference = load_reference_data(reference_file)
    guids = reference["GUID"].dropna().astype(str).tolist()
    visitors = fetch_visitors_data(api_host, date, guids)

    selected_guids = set(visitors["selected"])
    reference_guids = set(reference["GUID"])
    missing_guids = selected_guids - reference_guids
    if missing_guids:
        logging.warning(f"Зоны с GUID {missing_guids} отсутствуют в справочнике")

    table = create_visitors_table(reference, visitors, metric)
    console.print(table)


if __name__ == "__main__":
    main()
