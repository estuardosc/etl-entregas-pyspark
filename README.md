# ETL Entregas de Productos

Pipeline ETL con PySpark para procesar datos de entregas de productos. Usa OmegaConf para manejar la configuracion entre ambientes.

## Requisitos

- Python 3.9+
- Java 17

## Instalacion

```bash
pip install -r requirements.txt
```

## Uso

```bash
# Ejecucion basica
python main.py

# Filtrar por rango de fechas
python main.py filters.start_date=20250101 filters.end_date=20250630

# Filtrar por pais
python main.py filters.country=GT

# Cambiar ambiente
python main.py --env qa

# Ver configuracion sin ejecutar
python main.py --dry-run
```

## Estructura

```
etl-entregas-pyspark/
├── config/               # Archivos YAML de configuracion
├── data/
│   ├── raw/             # CSV de entrada
│   └── processed/       # Salida particionada por fecha
├── docs/                # Documentacion adicional
├── src/
│   └── etl_entregas.py  # Logica del ETL
├── tests/               # Tests unitarios
└── main.py              # Punto de entrada
```

## Que hace

1. Lee el CSV de entregas
2. Limpia datos (nulls, duplicados, tipos invalidos)
3. Filtra por fecha y pais segun parametros
4. Convierte unidades (CS = 20 unidades, ST = 1 unidad)
5. Clasifica entregas en RUTINA o BONIFICACION
6. Genera archivos particionados por fecha_proceso

## Configuracion

Los archivos YAML en `/config` controlan el comportamiento:

- `config.yaml` - valores por defecto
- `config_develop.yaml` - ambiente desarrollo
- `config_qa.yaml` - ambiente QA  
- `config_main.yaml` - produccion

Los parametros de linea de comandos tienen prioridad sobre los archivos.

## Salida

```
data/processed/develop/
├── fecha_proceso=20250114/
│   └── data.csv
├── fecha_proceso=20250217/
│   └── data.csv
├── ...
└── execution_metrics.json
```

## Tests

```bash
pytest tests/ -v
```

## Reglas de negocio

**Conversion de unidades:**
- CS (cajas) = 20 unidades
- ST = 1 unidad

**Tipos de entrega validos:**
- ZPRE, ZVE1 → Rutina
- Z04, Z05 → Bonificacion
- COBR y otros → Excluidos

**Paises:** GT, SV, HN, EC, PE, JM
