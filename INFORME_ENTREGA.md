# Informe de Entrega - Prueba Tecnica Data Engineer

**Autor:** Estuardo Santa Cruz  
**Fecha:** Diciembre 2025

---

## Descripcion del Proyecto

Este ETL procesa datos de entregas de productos para diferentes paises de Centroamerica y el Caribe. Lo construi con PySpark para manejar el volumen de datos y OmegaConf para que la configuracion sea flexible entre ambientes.

El pipeline lee un CSV con registros de entregas, aplica limpieza de datos, convierte las unidades a un estandar comun, clasifica los tipos de entrega y genera archivos particionados por fecha.

---

## Como ejecutarlo

```bash
# Instalacion
pip install -r requirements.txt

# Ejecucion basica
python main.py

# Filtrar por fechas especificas
python main.py filters.start_date=20250101 filters.end_date=20250630

# Filtrar solo Guatemala
python main.py filters.country=GT

# Cambiar de ambiente
python main.py --env qa
```

Nota: Requiere Java 17 instalado porque PySpark 3.5 ya no soporta versiones anteriores.

---

## Que hace el ETL

### Lectura y limpieza

El CSV original tiene 379 registros. Durante el procesamiento elimino:
- Registros donde el codigo de material viene vacio (18 registros)
- Tipos de entrega que no son relevantes como COBR (41 registros)  
- Duplicados exactos (197 registros)

Al final quedan 123 registros limpios distribuidos en 6 fechas diferentes.

### Conversion de unidades

El archivo trae cantidades en dos unidades distintas:
- **CS** (cajas): cada caja equivale a 20 unidades
- **ST** (unidades sueltas): se mantienen igual

Agregue una columna `cantidad_unidades` que normaliza todo a unidades individuales para poder comparar.

### Clasificacion de entregas

Segun el requerimiento, solo algunos tipos de entrega son validos:

- ZPRE y ZVE1 son entregas de **rutina** (el camion pasa normalmente)
- Z04 y Z05 son **bonificaciones** (producto extra o promociones)

Cree columnas booleanas para facilitar filtros en reportes: `es_entrega_rutina` y `es_entrega_bonificacion`.

### Particionamiento

Los archivos de salida se guardan en carpetas separadas por fecha:

```
data/processed/develop/
├── fecha_proceso=20250114/
│   └── data.csv
├── fecha_proceso=20250217/
│   └── data.csv
...
```

Esto facilita consultar solo los datos de una fecha especifica sin cargar todo.

---

## Columnas adicionales

Aparte de las columnas requeridas, agregue algunas que pueden servir para analisis:

| Columna | Para que sirve |
|---------|----------------|
| trimestre | Reportes financieros |
| dia_semana | Ver si hay dias con mas entregas |
| periodo_mes | Identificar si las entregas se concentran a inicio o fin de mes |
| rango_volumen | Categorizar entregas pequeñas vs grandes (BAJO, MEDIO, ALTO, MUY_ALTO) |
| es_alto_valor | Flag para entregas mayores a Q1,000 |
| precio_por_unidad | El precio dividido entre las unidades reales |

La idea es que estos campos faciliten hacer dashboards o reportes sin tener que recalcular.

---

## Configuracion con OmegaConf

Use OmegaConf porque permite tener un archivo base y luego sobreescribir valores segun el ambiente o desde linea de comandos.

```
config/
├── config.yaml          # Valores por defecto
├── config_develop.yaml  # Desarrollo (logs verbose)
├── config_qa.yaml       # QA
└── config_main.yaml     # Produccion
```

La jerarquia es: base → ambiente → argumentos CLI. Asi puedo correr el mismo codigo en diferentes ambientes solo cambiando un parametro.

---

## Estructura del proyecto

```
etl-entregas-pyspark/
├── config/              # Archivos YAML de configuracion
├── data/
│   ├── raw/            # CSV de entrada
│   └── processed/      # Salida particionada
├── docs/               # Documentacion y diagramas
├── src/
│   └── etl_entregas.py # Logica principal del ETL
├── tests/              # Tests unitarios
├── main.py             # Punto de entrada
└── requirements.txt
```

---

## Tests

Incluí tests para validar las transformaciones principales:
- Que la conversion de CS a unidades multiplique por 20
- Que el filtro de fechas funcione correctamente
- Que la clasificacion de tipos de entrega sea correcta

Para correrlos:
```bash
pytest tests/ -v
```

---

## Resultado final

El ETL genera un archivo `execution_metrics.json` con el resumen de cada ejecucion:

```json
{
  "status": "SUCCESS",
  "registros_iniciales": 379,
  "registros_finales": 123,
  "particiones_creadas": 6,
  "tiempo_ejecucion": "42 segundos"
}
```

---

**Estuardo Santa Cruz**  
Diciembre 2025
