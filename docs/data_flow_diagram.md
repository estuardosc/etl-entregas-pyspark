# Diagrama de Flujo de Datos - ETL Entregas de Productos

##  Índice
1. [Arquitectura General](#arquitectura-general)
2. [Diagrama de Alto Nivel](#diagrama-de-alto-nivel)
3. [Pipeline ETL Detallado](#pipeline-etl-detallado)
4. [Transformaciones de Datos](#transformaciones-de-datos)
5. [Calidad de Datos](#calidad-de-datos)
6. [Columnas Adicionales con Fundamento](#columnas-adicionales-con-fundamento)
7. [Configuración OmegaConf](#configuración-omegaconf)
8. [Estructura de Salida](#estructura-de-salida)

---

## Arquitectura General

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        ETL ENTREGAS DE PRODUCTOS v1.0                           │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                 │
│  ┌──────────────┐    ┌─────────────────┐    ┌────────────────────────────────┐ │
│  │   FUENTES    │    │    PROCESOS     │    │           DESTINOS             │ │
│  ├──────────────┤    ├─────────────────┤    ├────────────────────────────────┤ │
│  │              │    │                 │    │                                │ │
│  │  CSV Input   │───▶│  PySpark ETL    │───▶│  CSV Particionado por fecha    │ │
│  │              │    │                 │    │                                │ │
│  │  YAML Config │───▶│  OmegaConf      │───▶│  Métricas de Ejecución         │ │
│  │              │    │                 │    │                                │ │
│  │  CLI Args    │───▶│  Data Quality   │───▶│  Logs de Procesamiento         │ │
│  │              │    │                 │    │                                │ │
│  └──────────────┘    └─────────────────┘    └────────────────────────────────┘ │
│                                                                                 │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## Diagrama de Alto Nivel

```mermaid
flowchart TB
    subgraph INPUT[" ENTRADA"]
        CSV[("data_entrega_productos.csv<br/>379 registros")]
        CONFIG["config.yaml<br/>+ config_{env}.yaml"]
        CLI["CLI Arguments<br/>--env, filters.*"]
    end

    subgraph CONFIG_MERGE[" CONFIGURACIÓN"]
        OMEGA["OmegaConf<br/>Merge & Override"]
    end

    subgraph ETL[" PIPELINE ETL"]
        direction TB
        E["1. EXTRACT<br/>Lectura CSV"]
        DQ["2. DATA QUALITY<br/>• Null check<br/>• Valid types<br/>• Duplicates"]
        F["3. FILTERS<br/>• Date range<br/>• Country"]
        T["4. TRANSFORM<br/>• Unit conversion<br/>• Categories<br/>• Flags<br/>• Calculations"]
        S["5. STANDARDIZE<br/>Column naming"]
        L["6. LOAD<br/>Partition by date"]
    end

    subgraph OUTPUT[" SALIDA"]
        CSVOUT[("CSV Files<br/>Partitioned")]
        METRICS["execution_metrics.json"]
    end

    CSV --> E
    CONFIG --> OMEGA
    CLI --> OMEGA
    OMEGA --> E
    E --> DQ
    DQ --> F
    F --> T
    T --> S
    S --> L
    L --> CSVOUT
    L --> METRICS
```

---

## Pipeline ETL Detallado

```mermaid
flowchart TD
    subgraph EXTRACT[" EXTRACCIÓN"]
        E1["Leer CSV con PySpark"]
        E2["Inferir esquema automático"]
        E3["Validar estructura básica"]
    end

    subgraph QUALITY[" CALIDAD DE DATOS"]
        Q1["Eliminar material NULL/vacío"]
        Q2["Filtrar tipos válidos<br/>(ZPRE, ZVE1, Z04, Z05)"]
        Q3["Remover duplicados exactos"]
    end

    subgraph FILTER[" FILTRADO"]
        F1["Aplicar rango de fechas<br/>(start_date - end_date)"]
        F2["Filtrar por país<br/>(opcional)"]
    end

    subgraph TRANSFORM[" TRANSFORMACIÓN"]
        T1["Conversión de unidades<br/>CS×20, ST×1"]
        T2["Clasificación de entregas<br/>RUTINA/BONIFICACION"]
        T3["Cálculos de precios"]
        T4["Enriquecimiento temporal"]
        T5["Columnas analíticas"]
    end

    subgraph STANDARD[" ESTANDARIZACIÓN"]
        S1["Renombrar a snake_case"]
        S2["Ordenar columnas"]
        S3["Seleccionar schema final"]
    end

    subgraph LOAD[" CARGA"]
        L1["Particionar por fecha_proceso"]
        L2["Guardar CSV por partición"]
        L3["Generar métricas"]
    end

    E1 --> E2 --> E3 --> Q1
    Q1 --> Q2 --> Q3 --> F1
    F1 --> F2 --> T1
    T1 --> T2 --> T3 --> T4 --> T5 --> S1
    S1 --> S2 --> S3 --> L1
    L1 --> L2 --> L3
```

---

## Transformaciones de Datos

### Conversión de Unidades

```
┌─────────────────────────────────────────────────────────────────┐
│                    CONVERSIÓN DE UNIDADES                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│   ENTRADA                    FÓRMULA                 SALIDA     │
│   ────────                   ───────                 ──────     │
│                                                                 │
│   cantidad: 5       ×        factor: 20     =       100         │
│   unidad: CS                 (Caja)                 unidades    │
│                                                                 │
│   cantidad: 10      ×        factor: 1      =       10          │
│   unidad: ST                 (Unidad)               unidades    │
│                                                                 │
├─────────────────────────────────────────────────────────────────┤
│   cantidad_unidades = cantidad × CASE(unidad: CS→20, ST→1)      │
└─────────────────────────────────────────────────────────────────┘
```

### Clasificación de Entregas

```mermaid
flowchart LR
    subgraph INPUT["tipo_entrega"]
        ZPRE["ZPRE"]
        ZVE1["ZVE1"]
        Z04["Z04"]
        Z05["Z05"]
        COBR["COBR"]
    end

    subgraph CLASSIFY["Clasificación"]
        R["RUTINA"]
        B["BONIFICACION"]
        X["EXCLUIDO"]
    end

    subgraph OUTPUT["Columnas Generadas"]
        C1["categoria_entrega"]
        C2["es_entrega_rutina"]
        C3["es_entrega_bonificacion"]
    end

    ZPRE --> R
    ZVE1 --> R
    Z04 --> B
    Z05 --> B
    COBR --> X

    R --> C1
    R --> C2
    B --> C1
    B --> C3
```

---

## Calidad de Datos

```mermaid
flowchart TB
    subgraph INPUT[" Entrada: 379 registros"]
        I["CSV Raw Data"]
    end

    subgraph DQ[" Reglas de Calidad"]
        DQ1[" Material NULL/vacío<br/>-18 registros"]
        DQ2[" tipo_entrega inválido (COBR, otros)<br/>-41 registros"]
        DQ3[" Duplicados exactos<br/>-197 registros"]
    end

    subgraph OUTPUT[" Salida: 123 registros"]
        O["Clean Data"]
    end

    I --> DQ1
    DQ1 --> DQ2
    DQ2 --> DQ3
    DQ3 --> O

    style DQ1 fill:#ffcccc
    style DQ2 fill:#ffcccc
    style DQ3 fill:#ffcccc
    style OUTPUT fill:#ccffcc
```

### Métricas de Calidad

| Métrica | Valor | Descripción |
|---------|-------|-------------|
| registros_iniciales | 379 | Total de registros en CSV original |
| registros_null_material | 18 | Eliminados por código de material vacío |
| registros_tipo_invalido | 41 | Excluidos por tipo_entrega no válido |
| registros_duplicados | 197 | Removidos por ser duplicados exactos |
| registros_finales | 123 | Registros válidos procesados |

---

## Columnas Adicionales con Fundamento

###  Columnas Analíticas Agregadas

| Columna | Tipo | Fundamento de Negocio |
|---------|------|----------------------|
| `dia_proceso` | Integer | Permite análisis de patrones diarios de entrega |
| `dia_semana` | Integer | Identifica concentración de entregas por día (1-7) |
| `nombre_dia_semana` | String | Facilita reportes legibles (Lunes, Martes...) |
| `semana_del_anio` | Integer | Esencial para reportes semanales y comparativos YoY |
| `trimestre` | Integer | Fundamental para reportes financieros trimestrales |
| `periodo_mes` | String | Identifica patrones de inicio/fin de mes (metas, facturación) |
| `rango_volumen` | String | Categoriza entregas para planificación de capacidad |
| `es_alto_valor` | Boolean | Identifica entregas que requieren atención especial |
| `codigo_region` | String | Permite análisis geográfico por zona |
| `precio_por_unidad` | Double | Costo real por unidad individual (normalizado) |
| `es_bonificacion_gratuita` | Boolean | Identifica productos entregados sin costo |

### Diagrama de Columnas Enriquecidas

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                      ENRIQUECIMIENTO TEMPORAL                                │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   fecha_proceso: "20250325"                                                  │
│          │                                                                   │
│          ├──▶ anio_proceso: 2025                                            │
│          ├──▶ trimestre: 1                                                  │
│          ├──▶ mes_proceso: 3                                                │
│          ├──▶ semana_del_anio: 13                                           │
│          ├──▶ dia_proceso: 25                                               │
│          ├──▶ dia_semana: 3 (Martes)                                        │
│          ├──▶ nombre_dia_semana: "Martes"                                   │
│          └──▶ periodo_mes: "FIN_MES" (día >= 21)                            │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                      CATEGORIZACIÓN DE VOLUMEN                               │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   cantidad_unidades ──▶ rango_volumen                                       │
│                                                                              │
│   ├── 0-20 unidades    ──▶ "BAJO"                                           │
│   ├── 21-100 unidades  ──▶ "MEDIO"                                          │
│   ├── 101-500 unidades ──▶ "ALTO"                                           │
│   └── 500+ unidades    ──▶ "MUY_ALTO"                                       │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                      INDICADORES DE VALOR                                    │
├──────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│   precio_total > 1000  ──▶ es_alto_valor: TRUE                              │
│   precio = 0           ──▶ es_bonificacion_gratuita: TRUE                   │
│                                                                              │
└──────────────────────────────────────────────────────────────────────────────┘
```

---

## Configuración OmegaConf

```mermaid
flowchart LR
    subgraph BASE[" Base Config"]
        B["config.yaml<br/>Valores por defecto"]
    end

    subgraph ENV[" Environment Config"]
        E1["config_develop.yaml"]
        E2["config_qa.yaml"]
        E3["config_main.yaml"]
    end

    subgraph CLI[" CLI Overrides"]
        C["filters.start_date=...<br/>filters.country=..."]
    end

    subgraph FINAL[" Final Config"]
        F["OmegaConf<br/>DictConfig"]
    end

    B -->|"1. Load"| F
    E1 -->|"2. Merge"| F
    E2 -->|"2. Merge"| F
    E3 -->|"2. Merge"| F
    C -->|"3. Override"| F
```

### Jerarquía de Configuración

```
Prioridad (menor a mayor):
━━━━━━━━━━━━━━━━━━━━━━━━━━━
1. config.yaml (base)
     ↓
2. config_{env}.yaml (ambiente)
     ↓
3. CLI arguments (línea de comandos)  ← Máxima prioridad
```

### Ejemplos de Uso

```bash
# Ejecución por defecto (develop)
python main.py

# Ambiente específico
python main.py --env qa
python main.py --env main

# Filtro por fechas
python main.py filters.start_date=20250301 filters.end_date=20250331

# Filtro por país
python main.py filters.country=GT

# Combinación completa
python main.py --env main filters.country=SV filters.start_date=20250201
```

---

## Estructura de Salida

```
data/processed/{environment}/
│
├── fecha_proceso=20250114/
│   └── data.csv                 # 14 registros - Perú
│
├── fecha_proceso=20250217/
│   └── data.csv                 # 18 registros - Ecuador
│
├── fecha_proceso=20250314/
│   └── data.csv                 # 22 registros - Honduras
│
├── fecha_proceso=20250325/
│   └── data.csv                 # 57 registros - El Salvador
│
├── fecha_proceso=20250513/
│   └── data.csv                 # NN registros - Guatemala
│
├── fecha_proceso=20250602/
│   └── data.csv                 # 12 registros - Jamaica
│
└── execution_metrics.json       # Métricas de ejecución
```

### Schema de Salida Final (28 columnas)

```
┌────────────────────────────────────────────────────────────────────────────┐
│                           SCHEMA DE SALIDA                                 │
├────────────────────────────────────────────────────────────────────────────┤
│                                                                            │
│  IDENTIFICADORES GEOGRÁFICOS                                               │
│  ├── codigo_pais (string)          - Código ISO del país (GT, SV, HN...)  │
│  ├── nombre_pais (string)          - Nombre completo del país             │
│  └── codigo_region (string)        - Código de región (2 primeros chars)  │
│                                                                            │
│  IDENTIFICADORES TEMPORALES                                                │
│  ├── fecha_proceso (string)        - Fecha en formato YYYYMMDD            │
│  ├── anio_proceso (int)            - Año de proceso                       │
│  ├── trimestre (int)               - Trimestre (1-4)                      │
│  ├── mes_proceso (int)             - Mes de proceso (1-12)                │
│  ├── semana_del_anio (int)         - Semana del año (1-52)                │
│  ├── dia_proceso (int)             - Día del mes (1-31)                   │
│  ├── dia_semana (int)              - Día de la semana (1-7)               │
│  ├── nombre_dia_semana (string)    - Nombre del día                       │
│  └── periodo_mes (string)          - INICIO_MES/MEDIADOS_MES/FIN_MES      │
│                                                                            │
│  IDENTIFICADORES DE TRANSPORTE                                             │
│  ├── id_transporte (string)        - ID del transporte                    │
│  └── id_ruta (string)              - ID de la ruta                        │
│                                                                            │
│  PRODUCTO                                                                  │
│  └── codigo_material (string)      - Código del material/producto         │
│                                                                            │
│  TIPO DE ENTREGA                                                           │
│  ├── codigo_tipo_entrega (string)  - Código original (ZPRE, ZVE1...)      │
│  ├── categoria_entrega (string)    - RUTINA o BONIFICACION                │
│  ├── es_entrega_rutina (boolean)   - Flag para entregas de rutina         │
│  └── es_entrega_bonificacion (bool)- Flag para bonificaciones             │
│                                                                            │
│  CANTIDADES                                                                │
│  ├── cantidad_original (double)    - Cantidad en unidad original          │
│  ├── unidad_original (string)      - Unidad original (CS o ST)            │
│  ├── cantidad_unidades (double)    - Cantidad normalizada a unidades      │
│  └── rango_volumen (string)        - BAJO/MEDIO/ALTO/MUY_ALTO             │
│                                                                            │
│  PRECIOS E INDICADORES DE VALOR                                            │
│  ├── precio_unitario (double)      - Precio unitario original             │
│  ├── precio_por_unidad (double)    - Precio real por unidad individual    │
│  ├── precio_total (double)         - Precio × cantidad_unidades           │
│  ├── es_bonificacion_gratuita (bool)- TRUE si precio = 0                  │
│  └── es_alto_valor (boolean)       - TRUE si precio_total > 1000          │
│                                                                            │
│  METADATA                                                                  │
│  └── fecha_procesamiento_etl (timestamp) - Momento de ejecución del ETL   │
│                                                                            │
└────────────────────────────────────────────────────────────────────────────┘
```

---

## Métricas de Ejecución

El archivo `execution_metrics.json` contiene:

```json
{
  "status": "SUCCESS",
  "environment": "develop",
  "execution_time_seconds": 87.19,
  "timestamp": "2025-12-09T22:23:14",
  "filters_applied": {
    "start_date": "20250101",
    "end_date": "20250630",
    "country": null
  },
  "quality_metrics": {
    "registros_iniciales": 379,
    "registros_null_material": 18,
    "registros_duplicados": 197,
    "registros_tipo_invalido": 41,
    "registros_finales": 123
  },
  "output_info": {
    "output_path": "data/processed/develop",
    "partitions_created": 6,
    "dates": ["20250114", "20250217", "20250314", "20250325", "20250513", "20250602"]
  }
}
```

---

## Casos de Uso Analíticos

Las columnas adicionales permiten responder preguntas de negocio como:

1. **¿Qué días de la semana tienen más entregas?**
   - Usar: `dia_semana`, `nombre_dia_semana`

2. **¿Cómo se distribuyen las entregas a lo largo del mes?**
   - Usar: `periodo_mes` (INICIO/MEDIADOS/FIN)

3. **¿Cuál es el volumen típico por entrega?**
   - Usar: `rango_volumen`

4. **¿Qué porcentaje de entregas son de alto valor?**
   - Usar: `es_alto_valor`

5. **¿Cuántas bonificaciones gratuitas se entregan?**
   - Usar: `es_bonificacion_gratuita`

6. **Comparativa trimestral de entregas:**
   - Usar: `trimestre`, `anio_proceso`

7. **Análisis por región geográfica:**
   - Usar: `codigo_region`, `codigo_pais`
