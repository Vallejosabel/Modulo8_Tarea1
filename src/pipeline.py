
"""
Modulo 8 - Tarea 1

-1: Carga de Datos
  • Lee JSON Lines de interacciones de fans (Modulo8_Tarea1/input_json/*.json)
  • Lee CSV de datos demográficos de países (Modulo8_Tarea1/input_csv/country_data_v2.csv)
  • Escribe salidas:
      - Modulo8_Tarea1/output/raw/raw_interaccion_fans-*.jsonl
      - Modulo8_Tarea1/output/raw/raw_data_demografica_paises-*.jsonl

-2: Estandarización y Filtrado
  • Normaliza RaceID
  • Elimina registros cuando DeviceType == "Other"
  • Escribe salidas:
      - Modulo8_Tarea1/output/tratado/fans_filtrado_y_tratado-*.jsonl

-3: Enriquecimiento
  • Une por ViewerLocationCountry con país del CSV
  • Inserta objeto LocationData con 5 campos (Country, Capital, Continent, Main Official Language, Currency)
  • Escribe salidas:
      - Modulo8_Tarea1/output/enriquecido/fans_enriquecido_con_locationdata-*.jsonl
"""

import argparse
import json
import csv
import re
from typing import Iterable, Dict, Any, List

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions



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



def _norm_country_key(s: str) -> str:
    import re as _re
    s = str(s or "").strip().lower()
    s = _re.sub(r"\s+", " ", s)
    return s

def _project_location_fields(d: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "Country": d.get("Country"),
        "Capital": d.get("Capital"),
        "Continent": d.get("Continent"),
        "Main Official Language": d.get("Main Official Language"),
        "Currency": d.get("Currency"),
    }


#Pipeline principal
def run(argv=None):
    parser = argparse.ArgumentParser(description="HRL - Punto 1 + Punto 2 + Punto 3 (Apache Beam)")
    parser.add_argument("--json_glob", default="Modulo8_Tarea1/input_json/*.json")
    parser.add_argument("--csv_path",  default="Modulo8_Tarea1/input_csv/country_data_v2.csv")
    parser.add_argument("--out_raw",   default="Modulo8_Tarea1/output/raw")
    parser.add_argument("--out_tratado", default="Modulo8_Tarea1/output/tratado")
    parser.add_argument("--out_enriquecido", default="Modulo8_Tarea1/output/enriquecido")
    parser.add_argument("--do_p1", action="store_true", help="Ejecutar solo Punto 1")
    parser.add_argument("--do_p2", action="store_true", help="Ejecutar solo Punto 2")
    parser.add_argument("--do_p3", action="store_true", help="Ejecutar solo Punto 3")
    known_args, pipeline_args = parser.parse_known_args(argv)

  
    if not (known_args.do_p1 or known_args.do_p2 or known_args.do_p3):
        known_args.do_p1 = known_args.do_p2 = known_args.do_p3 = True

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
    out_enriquecido = known_args.out_enriquecido.rstrip("/")

    with beam.Pipeline(options=pipeline_options) as p:
        
        fans = (p
                | "ReadFansJSON" >> beam.io.ReadFromText(known_args.json_glob)
                | "ParseFansJSON" >> beam.ParDo(ParseJsonLine()))
        countries = (p
                     | "ReadCountriesCSV" >> beam.io.ReadFromText(
                         known_args.csv_path, skip_header_lines=1)
                     | "ParseCountriesCSV" >> beam.ParDo(ParseCsvWithHeader(csv_header)))

        #Tarea 1 - Punto 1
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

        #Tarea 1 - Punto 2
        fans_filtrado_y_tratado = None
        if known_args.do_p2:
            fans_filtrado_y_tratado = (
                fans
                | "StdRaceID" >> beam.ParDo(StandardizeRaceId())
                | "FilterDeviceOther" >> beam.ParDo(FilterDeviceOther())
            )
            _ = (fans_filtrado_y_tratado
                 | "FansToJSON_TRAT" >> beam.Map(json.dumps, ensure_ascii=False)
                 | "WriteFansTRAT" >> beam.io.WriteToText(
                     file_path_prefix=f"{out_tratado}/fans_filtrado_y_tratado",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))

        #Tarea 1 - Punto 3
        if known_args.do_p3:
            countries_kv = (
                countries
                | "KeyCountriesByName" >> beam.Map(
                    lambda d: (_norm_country_key(d.get("Country")), _project_location_fields(d))
                )
            )
            countries_dict_side = beam.pvalue.AsDict(countries_kv)

            if fans_filtrado_y_tratado is not None:
                base_for_enrichment = fans_filtrado_y_tratado
            else:
                base_for_enrichment = (
                    fans
                    | "P3_StdRaceID" >> beam.ParDo(StandardizeRaceId())
                    | "P3_FilterDeviceOther" >> beam.ParDo(FilterDeviceOther())
                )

            fans_enriquecido = (
                base_for_enrichment
                | "EnrichWithLocationData" >> beam.Map(
                    lambda rec, cdict: (rec.update({
                        "LocationData": cdict.get(_norm_country_key(rec.get("ViewerLocationCountry")))
                    }) or rec),
                    cdict=countries_dict_side
                )
            )

            _ = (fans_enriquecido
                 | "FansToJSON_ENR" >> beam.Map(json.dumps, ensure_ascii=False)
                 | "WriteFansENR" >> beam.io.WriteToText(
                     file_path_prefix=f"{out_enriquecido}/fans_enriquecido_con_locationdata",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))
                

if __name__ == "__main__":
    run()
