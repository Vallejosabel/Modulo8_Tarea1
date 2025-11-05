#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import csv
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

def run(argv=None):
    parser = argparse.ArgumentParser(description="HRL - Punto 1: Carga de Datos (Apache Beam)")
    parser.add_argument("--json_glob", default="input_json/*.json")
    parser.add_argument("--csv_path", default="input_csv/country_data_v2.csv")
    parser.add_argument("--out_dir",  default="output/raw")
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args, save_main_session=True)

    csv_header = [
        "Country","Capital","GDP (Nominal, 2024, in billions USD)",
        "Population (2024, in millions)","Pop. Growth Rate (2024, %)",
        "Life Expectancy (2024, years)","Median Age (2024, years)",
        "Urban Population (2022, %)","Continent","Main Official Language","Currency",
    ]
    out_dir = known_args.out_dir.rstrip("/")

    with beam.Pipeline(options=pipeline_options) as p:
        fans = (
            p
            | "ReadJSON"  >> beam.io.ReadFromText(file_pattern=known_args.json_glob)
            | "ParseJSON" >> beam.ParDo(ParseJsonLine())
        )
        _ = (
            fans
            | "FansToJsonStr" >> beam.Map(json.dumps, ensure_ascii=False)
            | "WriteFansRaw"  >> beam.io.WriteToText(
                file_path_prefix=f"{out_dir}/raw_fans",
                file_name_suffix=".jsonl",
                shard_name_template="-SSSSS"
            )
        )

        countries = (
            p
            | "ReadCSV"  >> beam.io.ReadFromText(known_args.csv_path, skip_header_lines=1)
            | "ParseCSV" >> beam.ParDo(ParseCsvWithHeader(csv_header))
        )
        _ = (
            countries
            | "CountriesToJsonStr" >> beam.Map(json.dumps, ensure_ascii=False)
            | "WriteCountriesRaw"  >> beam.io.WriteToText(
                file_path_prefix=f"{out_dir}/raw_countries",
                file_name_suffix=".jsonl",
                shard_name_template="-SSSSS"
            )
        )

if __name__ == "__main__":
    run()


