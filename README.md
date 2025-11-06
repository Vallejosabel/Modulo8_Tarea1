## Descripción del repositorio
Tarea número 1 para el Módulo 8 del diplomado de Ingeniería de Datos.  
El objetivo principal del desarrollo es mostrar:

1. Leer **múltiples archivos JSON** (`input_json/`)  que representan interacciones de fans.
2. Leer un **archivo CSV** (`input_csv/`) con información demográfica de países.
3. Enriquecer cada registro JSON con los datos del país.
4. Aplicar transformaciones sobre el flujo principal (filtros, formateo de IDs, selección de columnas).


## Estructura del repositorio

```text
Modulo8_Tarea1/
├── input_json/          # JSONs de entrada
│   ├── ...              
├── input_csv/           # CSV de enriquecimiento
│   └── ...              
├── output/              # Carpeta de salida (Se genera al ejecutar pipeline)
│   └── raw              # JSONs de salida (interacciones fans y datos demograficos)
│   └── tratado          # JSONs de salida Estandarizado y Filtrado
│   └── enriquecido      # JSONs de salida Estandarizado y Filtrado
├── src/
│   └── pipeline.py      # Script principal
├── requirements.txt     # Arvhivo de dependencias del pipeline
└── README.md            # Archivo Readme
```

---

## Ejecución del Pipeline

Para poder ejecutar el pipeline en colab se deben ejecutar los siguientes comandos:

**Clonar el repo**:

```
!git clone https://github.com/Vallejosabel/Modulo8_Tarea1.git
```

**Instalar requirements**:

```
%cd Modulo8_Tarea1
!pip install -r requirements.txt
```

En este punto pedirá reiniciar la sesión, lo cual permitimos.

La sesión se reiniciará.


**Ejecución de Pipeline**:

```
!python Modulo8_Tarea1/src/pipeline.py
```


Parámetros que están dentro del código:
* **--json_glob"**: "Modulo8_Tarea1/input_json/*.json" -  Carpeta de Json de entrada.
* **--csv_path**: "Modulo8_Tarea1/input_csv/country_data_v2.csv" - Carpeta de Csv de entrada.
* **--out_raw**: "Modulo8_Tarea1/output/raw" - Carpeta de Json y Csv de salida.
* **--out_tratado**: "Modulo8_Tarea1/output/tratado" - Carpeta de Json estandarizado y filtrado.
* **--out_enriquecido**: "Modulo8_Tarea1/output/enriquecido" - Carpeta de Json enriquecido.

__Para el Punto 1 Carga de datos, la salida se encontrará en Modulo8_Tarea1/output/raw.__
__Para el Punto 2 Estandarización y Filtrado de los datos JSON, la salida se encontrará en Modulo8_Tarea1/output/tratado.__
__Para el Punto 3 Enriquecimiento de los datos, la salida se encontrará en Modulo8_Tarea1/output/enriquecido.__

