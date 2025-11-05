
"""
Modulo 8 - Tarea 1

-1: Carga de Datos
  • Lee JSON Lines de interacciones de fans (input_json/*.json)
  • Lee CSV de datos demográficos de países (input_csv/country_data_v2.csv)
  • Escribe salidas RAW:
      - output/raw/raw_interaccion_fans-*.jsonl
      - output/raw/raw_data_demografica_paises-*.jsonl

-2: Estandarización y Filtrado
  • Normaliza RaceID 
  • Elimina registros cuando DeviceType == "Other"
  • Escribe salidas:
      - output/tratado/fans_std_filtered-*.jsonl
"""

import argparse
import json
import csv
import re
from typing import Iterable, Dict, Any, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions


# ----------------- Utilidades -----------------
class ParseJsonLine(beam.DoFn):
    def process(self, line: str) -> Iterable[Dict[str, Any]]:
        s = (line or "").strip()
        if not s:
            return
        try:
            yield json.loads(s)
        except Exception:
            beam.metrics.Metrics.counter("parse_json", "invalid").inc()


class ParseCsvWithHeader(beam.DoFn):
    def __init__(self, header: List[str]):
        self.header = header
    def process(self, line: str) -> Iterable[Dict[str, Any]]:
        row = next(csv.reader([line]))
        if len(row) != len(self.header):
            beam.metrics.Metrics.counter("parse_csv", "bad_row_len").inc()
            return
        yield dict(zip(self.header, row))


# ----------------- Estandarización / Filtro -----------------
RACEID_PATTERN = re.compile(r"([A-Za-z]+)[^\d]*?(\d+)", re.UNICODE)

def normalize_race_id(raw: str) -> str:
    if not raw:
        return raw
    m = RACEID_PATTERN.search(str(raw))
    if m:
        alpha, num = m.group(1).lower(), m.group(2)
        return f"{alpha}{num}"
    return re.sub(r"[\s:_\-]+", "", str(raw).lower())


class StandardizeRaceId(beam.DoFn):
    def process(self, record: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        record["RaceID"] = normalize_race_id(record.get("RaceID"))
        yield record


class FilterDeviceOther(beam.DoFn):
    def process(self, record: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if str(record.get("DeviceType", "")).strip() == "Other":
            beam.metrics.Metrics.counter("filter", "device_other").inc()
            return
        yield record


# ----------------- Pipeline principal -----------------
def run(argv=None):
    parser = argparse.ArgumentParser(description="HRL - Punto 1 + Punto 2 (Apache Beam)")
    parser.add_argument("--json_glob", default="Modulo8_Tarea1/input_json/*.json")
    parser.add_argument("--csv_path",  default="Modulo8_Tarea1/input_csv/country_data_v2.csv")
    parser.add_argument("--out_raw",   default="Modulo8_Tarea1/output/raw")
    parser.add_argument("--out_tratado",   default="Modulo8_Tarea1/output/tratado")
    parser.add_argument("--do_p1", action="store_true", help="Ejecutar solo Punto 1")
    parser.add_argument("--do_p2", action="store_true", help="Ejecutar solo Punto 2")
    known_args, pipeline_args = parser.parse_known_args(argv)

    if not (known_args.do_p1 or known_args.do_p2):
        known_args.do_p1 = known_args.do_p2 = True

    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    csv_header = [
        "Country","Capital","GDP (Nominal, 2024, in billions USD)",
        "Population (2024, in millions)","Pop. Growth Rate (2024, %)",
        "Life Expectancy (2024, years)","Median Age (2024, years)",
        "Urban Population (2022, %)","Continent",
        "Main Official Language","Currency",
    ]

    out_raw = known_args.out_raw.rstrip("/")
    out_tratado = known_args.out_tratado.rstrip("/")

    with beam.Pipeline(options=pipeline_options) as p:
        # Lectura común
        fans = (p
                | "ReadFansJSON" >> beam.io.ReadFromText(known_args.json_glob)
                | "ParseFansJSON" >> beam.ParDo(ParseJsonLine()))
        countries = (p
                     | "ReadCountriesCSV" >> beam.io.ReadFromText(
                         known_args.csv_path, skip_header_lines=1)
                     | "ParseCountriesCSV" >> beam.ParDo(ParseCsvWithHeader(csv_header)))

        # ----------- Punto 1: RAW -----------
        if known_args.do_p1:
            _ = (fans
                 | "FansToJSON_RAW" >> beam.Map(json.dumps, ensure_ascii=False)
                 | "WriteFansRAW" >> beam.io.WriteToText(
                     file_path_prefix=f"{out_raw}/raw_interaccion_fans",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))
            _ = (countries
                 | "CountriesToJSON_RAW" >> beam.Map(json.dumps, ensure_ascii=False)
                 | "WriteCountriesRAW" >> beam.io.WriteToText(
                     file_path_prefix=f"{out_raw}/raw_data_demografica_paises",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))

        # ----------- Punto 2: Tratado -----------
        if known_args.do_p2:
            fans_std_filtered = (
                fans
                | "StdRaceID" >> beam.ParDo(StandardizeRaceId())
                | "FilterDeviceOther" >> beam.ParDo(FilterDeviceOther())
            )
            _ = (fans_std_filtered
                 | "FansToJSON_CUR" >> beam.Map(json.dumps, ensure_ascii=False)
                 | "WriteFansCUR" >> beam.io.WriteToText(
                     file_path_prefix=f"{out_tratado}/fans_std_filtered",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))


if __name__ == "__main__":
    run()
