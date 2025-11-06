
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


#Recibe linea por linea desde json, para convertirlo en dict
class ParseJsonLine(beam.DoFn):
    def process(self, line: str) -> Iterable[Dict[str, Any]]:
        s = (line or "").strip()
        if not s:
            return
        try:
            yield json.loads(s)
        except Exception:
            beam.metrics.Metrics.counter("parse_json", "invalid").inc()

#Clase responsable que convierte  cada linea del archivo csv en un dict
class ParseCsvWithHeader(beam.DoFn):
    def __init__(self, header: List[str]): #Recibe la lista de columnas y la guarda como atributo.
        self.header = header
    def process(self, line: str) -> Iterable[Dict[str, Any]]: #Se ejecuta por cada linea.
        row = next(csv.reader([line]))
        if len(row) != len(self.header):
            beam.metrics.Metrics.counter("parse_csv", "bad_row_len").inc()
            return
        yield dict(zip(self.header, row))


#Patrón de expresión regular, que se usa sobre RaceID
RACEID_PATTERN = re.compile(r"([A-Za-z]+)[^\d]*?(\d+)", re.UNICODE)


#Función de Normalización sobre campo RaceID
def normalize_race_id(raw: str) -> str:
    if not raw:
        return raw
    m = RACEID_PATTERN.search(str(raw))
    if m:
        alpha, num = m.group(1).lower(), m.group(2)
        return f"{alpha}{num}"
    return re.sub(r"[\s:_\-]+", "", str(raw).lower())

#Clase de Estrandarizacion, llama a la funcion normalize_race_id
class StandardizeRaceId(beam.DoFn):
    def process(self, record: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        record["RaceID"] = normalize_race_id(record.get("RaceID"))
        yield record

#Clase que filtra cuando DeviceType es igual a "Other", para ser descartado.
class FilterDeviceOther(beam.DoFn):
    def process(self, record: Dict[str, Any]) -> Iterable[Dict[str, Any]]:
        if str(record.get("DeviceType", "")).strip() == "Other":
            beam.metrics.Metrics.counter("filter", "device_other").inc()
            return
        yield record


#Función de normalización de texto, prepara las claves país para luego comparar Country con ViewerLocationCountry
def _norm_country_key(s: str) -> str:
    import re as _re
    s = str(s or "").strip().lower()
    s = _re.sub(r"\s+", " ", s)
    return s

#Función que crea un sub diccionario solo con los campos solicitados en el punto 3
def _project_location_fields(d: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "Country": d.get("Country"),
        "Capital": d.get("Capital"),
        "Continent": d.get("Continent"),
        "Main Official Language": d.get("Main Official Language"),
        "Currency": d.get("Currency"),
    }


#Función principal del pipeline
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

    #Valida si hay algun argumento de ejecución, agregada por el usuario en la ejecución del script.
    #Si no hay argumentos, asume que debe ejecutar todo
    if not (known_args.do_p1 or known_args.do_p2 or known_args.do_p3):
        known_args.do_p1 = known_args.do_p2 = known_args.do_p3 = True

    #Permite la captura de parámetros de ejecución al momento de lanzar el pipeline.
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    #Define esquema que se usará para transformar cada linea del csv en un diccionario.
    csv_header = [
        "Country","Capital","GDP (Nominal, 2024, in billions USD)",
        "Population (2024, in millions)","Pop. Growth Rate (2024, %)",
        "Life Expectancy (2024, years)","Median Age (2024, years)",
        "Urban Population (2022, %)","Continent",
        "Main Official Language","Currency",
    ]
    
    #Normalización de rutas a utilizar.
    out_raw = known_args.out_raw.rstrip("/")
    out_tratado = known_args.out_tratado.rstrip("/")
    out_enriquecido = known_args.out_enriquecido.rstrip("/")

    #Crea el entorno del pipeline
    with beam.Pipeline(options=pipeline_options) as p:
        
        fans = (p
                #Lee y convierte los archivos json de fans a diccionarios.
                | "ReadFansJSON" >> beam.io.ReadFromText(known_args.json_glob)
                | "ParseFansJSON" >> beam.ParDo(ParseJsonLine()))
        countries = (p
                     #Lee y convierte los archivos csv de países en diccionarios.
                     | "ReadCountriesCSV" >> beam.io.ReadFromText(
                         known_args.csv_path, skip_header_lines=1)
                     | "ParseCountriesCSV" >> beam.ParDo(ParseCsvWithHeader(csv_header)))

        #Tarea 1 - Punto 1
        if known_args.do_p1: #Se ejecuta si se le pasa el argumento --do_p1 o si el pipeline se ejecuta sin argumentos.
            _ = (fans #PCollection
                 | "FansToJSON_RAW" >> beam.Map(json.dumps, ensure_ascii=False)
                 | "WriteFansRAW" >> beam.io.WriteToText(
                     file_path_prefix=f"{out_raw}/raw_interaccion_fans",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))
            _ = (countries #PCollection
                 | "CountriesToJSON_RAW" >> beam.Map(json.dumps, ensure_ascii=False) #Convierte el csv de paises en .jsonl
                 | "WriteCountriesRAW" >> beam.io.WriteToText( #Escribe y guarda en output/raw
                     file_path_prefix=f"{out_raw}/raw_data_demografica_paises",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))

        #Tarea 1 - Punto 2
        fans_filtrado_y_tratado = None
        if known_args.do_p2: #Se ejecuta si se le pasa el argumento --do_p2 o si el pipeline se ejecuta sin argumentos.
            fans_filtrado_y_tratado = (
                fans
                | "StdRaceID" >> beam.ParDo(StandardizeRaceId()) #Normalización campo RaceID
                | "FilterDeviceOther" >> beam.ParDo(FilterDeviceOther()) #Elimina registros de DeviceType == Other
            )
            _ = (fans_filtrado_y_tratado
                 | "FansToJSON_TRAT" >> beam.Map(json.dumps, ensure_ascii=False) #Convierte cada registro en texto json
                 | "WriteFansTRAT" >> beam.io.WriteToText( #Escribe y guarda los datos en output/tratado
                     file_path_prefix=f"{out_tratado}/fans_filtrado_y_tratado",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))

        #Tarea 1 - Punto 3
        if known_args.do_p3: #Se ejecuta si se le pasa el argumento --do_p3 o si el pipeline se ejecuta sin argumentos.
            #Construye un diccionario de paises
            countries_kv = (
                countries
                | "KeyCountriesByName" >> beam.Map(
                    lambda d: (_norm_country_key(d.get("Country")), _project_location_fields(d))
                )
            )
            countries_dict_side = beam.pvalue.AsDict(countries_kv)

            #Elige la fuente a enriqucer
            if fans_filtrado_y_tratado is not None:
                base_for_enrichment = fans_filtrado_y_tratado
            else:
                base_for_enrichment = (
                    fans
                    | "P3_StdRaceID" >> beam.ParDo(StandardizeRaceId())
                    | "P3_FilterDeviceOther" >> beam.ParDo(FilterDeviceOther())
                )

            #Enriquece cada fan con LocationData
            fans_enriquecido = (
                base_for_enrichment
                | "EnrichWithLocationData" >> beam.Map(
                    lambda rec, cdict: (rec.update({
                        "LocationData": cdict.get(_norm_country_key(rec.get("ViewerLocationCountry")))
                    }) or rec),
                    cdict=countries_dict_side
                )
            )
            #Escribe la data enriquecida en output/enriquecido/fans_enriquecido_con_locationdata*.jsonl
            _ = (fans_enriquecido
                 | "FansToJSON_ENR" >> beam.Map(json.dumps, ensure_ascii=False)
                 | "WriteFansENR" >> beam.io.WriteToText(
                     file_path_prefix=f"{out_enriquecido}/fans_enriquecido_con_locationdata",
                     file_name_suffix=".jsonl",
                     shard_name_template="-SSSSS"))
                
#Ejecución de pipeline completo
if __name__ == "__main__":
    run()
