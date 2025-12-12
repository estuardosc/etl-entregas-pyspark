"""
ETL Module - Procesamiento de Entregas de Productos
====================================================
Modulo principal que contiene las funciones de extraccion, transformacion y carga
para el procesamiento de datos de entregas de productos.

Author: Estuardo Santa Cruz
Version: 1.0.0
"""

import os
import sys
import platform

# Configuración para Windows - evitar error de HADOOP_HOME
if platform.system() == "Windows":
    # Crear directorio temporal para Hadoop si no existe
    hadoop_home = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "hadoop")
    hadoop_bin = os.path.join(hadoop_home, "bin")
    os.makedirs(hadoop_bin, exist_ok=True)
    
    # Configurar variables de entorno
    os.environ["HADOOP_HOME"] = hadoop_home
    os.environ["hadoop.home.dir"] = hadoop_home
    
    # Agregar al PATH
    if hadoop_bin not in os.environ.get("PATH", ""):
        os.environ["PATH"] = hadoop_bin + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, 
    IntegerType, BooleanType, TimestampType
)
from omegaconf import DictConfig
from typing import Optional, List, Tuple
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class EntregasETL:
    """
    Clase principal para el procesamiento ETL de entregas de productos.
    
    Attributes:
        config (DictConfig): Configuración cargada desde YAML via OmegaConf
        spark (SparkSession): Sesión de Spark
    """
    
    def __init__(self, config: DictConfig):
        """
        Inicializa el ETL con la configuración proporcionada.
        
        Args:
            config: Configuración de OmegaConf
        """
        self.config = config
        self.spark = self._create_spark_session()
        self._setup_logging()
        
    def _create_spark_session(self) -> SparkSession:
        """Crea y configura la sesión de Spark."""
        builder = SparkSession.builder \
            .appName(self.config.spark.app_name) \
            .master(self.config.spark.master)
        
        # Aplicar configuraciones adicionales
        for key, value in self.config.spark.configs.items():
            builder = builder.config(key, str(value))
        
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel(self.config.spark.log_level)
        
        logger.info(f"SparkSession creada: {self.config.spark.app_name}")
        return spark
    
    def _setup_logging(self):
        """Configura el sistema de logging."""
        log_level = getattr(logging, self.config.logging.level.upper(), logging.INFO)
        logging.basicConfig(
            level=log_level,
            format=self.config.logging.format
        )
    
    # =========================================================================
    # EXTRACCIÓN
    # =========================================================================
    
    def extract(self) -> DataFrame:
        """
        Lee el archivo CSV de entrada.
        
        Returns:
            DataFrame con los datos crudos
        """
        logger.info(f"Leyendo archivo: {self.config.paths.input_file}")
        
        df = self.spark.read \
            .option("header", "true") \
            .option("inferSchema", "false") \
            .option("quote", '"') \
            .option("escape", '"') \
            .csv(self.config.paths.input_file)
        
        record_count = df.count()
        logger.info(f"Registros leídos: {record_count}")
        
        return df
    
    # =========================================================================
    # TRANSFORMACIÓN - CALIDAD DE DATOS
    # =========================================================================
    
    def apply_data_quality(self, df: DataFrame) -> Tuple[DataFrame, dict]:
        """
        Aplica reglas de calidad de datos y elimina anomalías.
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            Tuple con DataFrame limpio y diccionario de métricas de calidad
        """
        quality_metrics = {
            "registros_iniciales": df.count(),
            "registros_null_material": 0,
            "registros_duplicados": 0,
            "registros_tipo_invalido": 0,
            "registros_finales": 0
        }
        
        logger.info("Aplicando reglas de calidad de datos...")
        
        # 1. Eliminar registros con material nulo o vacío
        if self.config.data_quality.remove_null_material:
            before_count = df.count()
            df = df.filter(
                (F.col("material").isNotNull()) & 
                (F.trim(F.col("material")) != "")
            )
            quality_metrics["registros_null_material"] = before_count - df.count()
            logger.info(f"Registros eliminados por material nulo: {quality_metrics['registros_null_material']}")
        
        # 2. Filtrar solo tipos de entrega válidos (ZPRE, ZVE1, Z04, Z05)
        valid_types = (
            list(self.config.business_rules.delivery_types.routine) + 
            list(self.config.business_rules.delivery_types.bonus)
        )
        before_count = df.count()
        df = df.filter(F.col("tipo_entrega").isin(valid_types))
        quality_metrics["registros_tipo_invalido"] = before_count - df.count()
        logger.info(f"Registros eliminados por tipo_entrega inválido: {quality_metrics['registros_tipo_invalido']}")
        
        # 3. Eliminar duplicados exactos
        if self.config.data_quality.remove_duplicates:
            before_count = df.count()
            df = df.dropDuplicates()
            quality_metrics["registros_duplicados"] = before_count - df.count()
            logger.info(f"Registros duplicados eliminados: {quality_metrics['registros_duplicados']}")
        
        # 4. Validar códigos de país
        if self.config.data_quality.validate_country_codes:
            valid_countries = list(self.config.data_quality.valid_countries)
            df = df.filter(F.upper(F.col("pais")).isin(valid_countries))
        
        quality_metrics["registros_finales"] = df.count()
        logger.info(f"Registros después de calidad: {quality_metrics['registros_finales']}")
        
        return df, quality_metrics
    
    # =========================================================================
    # TRANSFORMACIÓN - FILTROS
    # =========================================================================
    
    def apply_filters(self, df: DataFrame) -> DataFrame:
        """
        Aplica filtros de fecha y país según configuración.
        
        Args:
            df: DataFrame de entrada
            
        Returns:
            DataFrame filtrado
        """
        start_date = self.config.filters.start_date
        end_date = self.config.filters.end_date
        country = self.config.filters.country
        
        logger.info(f"Aplicando filtros - Fechas: {start_date} a {end_date}, País: {country or 'TODOS'}")
        
        # Filtro por rango de fechas
        df = df.filter(
            (F.col("fecha_proceso") >= start_date) & 
            (F.col("fecha_proceso") <= end_date)
        )
        
        # Filtro por país (si se especifica)
        if country is not None:
            df = df.filter(F.upper(F.col("pais")) == country.upper())
        
        filtered_count = df.count()
        logger.info(f"Registros después de filtros: {filtered_count}")
        
        return df
    
    # =========================================================================
    # TRANSFORMACIÓN - ENRIQUECIMIENTO
    # =========================================================================
    
    def transform(self, df: DataFrame) -> DataFrame:
        """
        Aplica todas las transformaciones de negocio al DataFrame.
        
        Args:
            df: DataFrame filtrado
            
        Returns:
            DataFrame transformado con columnas adicionales
        """
        logger.info("Aplicando transformaciones de negocio...")
        
        # Obtener configuraciones
        units_conv = self.config.business_rules.units_conversion
        routine_types = list(self.config.business_rules.delivery_types.routine)
        bonus_types = list(self.config.business_rules.delivery_types.bonus)
        country_names = dict(self.config.country_names)
        
        # 1. Castear columnas numéricas
        df = df.withColumn("precio", F.col("precio").cast(DoubleType())) \
               .withColumn("cantidad", F.col("cantidad").cast(DoubleType()))
        
        # 2. Convertir cantidad a unidades (CS=20, ST=1)
        df = df.withColumn(
            "cantidad_unidades",
            F.when(F.col("unidad") == "CS", F.col("cantidad") * units_conv.CS)
             .when(F.col("unidad") == "ST", F.col("cantidad") * units_conv.ST)
             .otherwise(F.col("cantidad"))
        )
        
        # 3. Clasificar tipo de entrega
        df = df.withColumn(
            "categoria_entrega",
            F.when(F.col("tipo_entrega").isin(routine_types), "RUTINA")
             .when(F.col("tipo_entrega").isin(bonus_types), "BONIFICACION")
             .otherwise("OTRO")
        )
        
        # 4. Columnas flag por tipo de entrega
        df = df.withColumn(
            "es_entrega_rutina",
            F.when(F.col("tipo_entrega").isin(routine_types), True).otherwise(False)
        )
        
        df = df.withColumn(
            "es_entrega_bonificacion",
            F.when(F.col("tipo_entrega").isin(bonus_types), True).otherwise(False)
        )
        
        # 5. Calcular precio total
        df = df.withColumn(
            "precio_total",
            F.round(F.col("precio") * F.col("cantidad_unidades"), 2)
        )
        
        # 6. Agregar nombre del país
        country_mapping = F.create_map([F.lit(x) for pair in country_names.items() for x in pair])
        df = df.withColumn(
            "nombre_pais",
            country_mapping[F.upper(F.col("pais"))]
        )
        
        # 7. Agregar timestamp de procesamiento ETL
        df = df.withColumn(
            "fecha_procesamiento_etl",
            F.current_timestamp()
        )
        
        # 8. Columna adicional: Precio unitario real (por unidad individual)
        df = df.withColumn(
            "precio_por_unidad",
            F.when(F.col("cantidad_unidades") > 0, 
                   F.round(F.col("precio") / F.col("cantidad_unidades"), 4))
             .otherwise(0)
        )
        
        # 9. Columna adicional: Indicador de bonificación (precio = 0)
        df = df.withColumn(
            "es_bonificacion_gratuita",
            F.when(F.col("precio") == 0, True).otherwise(False)
        )
        
        # 10. Columna adicional: Año y mes para análisis temporal
        df = df.withColumn(
            "anio_proceso",
            F.substring(F.col("fecha_proceso"), 1, 4).cast(IntegerType())
        )
        df = df.withColumn(
            "mes_proceso",
            F.substring(F.col("fecha_proceso"), 5, 2).cast(IntegerType())
        )
        
        # =====================================================
        # COLUMNAS ADICIONALES CON FUNDAMENTO (PUNTOS EXTRAS)
        # =====================================================
        
        # 11. Día del mes - Para análisis de patrones de entrega
        df = df.withColumn(
            "dia_proceso",
            F.substring(F.col("fecha_proceso"), 7, 2).cast(IntegerType())
        )
        
        # 12. Día de la semana (1=Lunes, 7=Domingo)
        # Fundamento: Permite identificar patrones de entrega por día de semana
        # Útil para planificación de rutas y recursos
        df = df.withColumn(
            "fecha_date",
            F.to_date(F.col("fecha_proceso"), "yyyyMMdd")
        )
        df = df.withColumn(
            "dia_semana",
            F.dayofweek(F.col("fecha_date"))
        )
        df = df.withColumn(
            "nombre_dia_semana",
            F.when(F.col("dia_semana") == 1, "Domingo")
             .when(F.col("dia_semana") == 2, "Lunes")
             .when(F.col("dia_semana") == 3, "Martes")
             .when(F.col("dia_semana") == 4, "Miércoles")
             .when(F.col("dia_semana") == 5, "Jueves")
             .when(F.col("dia_semana") == 6, "Viernes")
             .otherwise("Sábado")
        )
        
        # 13. Semana del año
        # Fundamento: Facilita reportes semanales y comparativos año vs año
        df = df.withColumn(
            "semana_del_anio",
            F.weekofyear(F.col("fecha_date"))
        )
        
        # 14. Trimestre
        # Fundamento: Esencial para reportes financieros y análisis estacionales
        df = df.withColumn(
            "trimestre",
            F.quarter(F.col("fecha_date"))
        )
        
        # 15. Indicador inicio/fin de mes
        # Fundamento: Las entregas suelen concentrarse al inicio o fin de mes
        # por temas de facturación y cumplimiento de metas
        df = df.withColumn(
            "periodo_mes",
            F.when(F.col("dia_proceso") <= 10, "INICIO_MES")
             .when(F.col("dia_proceso") >= 21, "FIN_MES")
             .otherwise("MEDIADOS_MES")
        )
        
        # 16. Rango de volumen de entrega
        # Fundamento: Categoriza entregas para análisis de distribución
        # y planificación de capacidad de transporte
        df = df.withColumn(
            "rango_volumen",
            F.when(F.col("cantidad_unidades") <= 20, "BAJO")
             .when(F.col("cantidad_unidades") <= 100, "MEDIO")
             .when(F.col("cantidad_unidades") <= 500, "ALTO")
             .otherwise("MUY_ALTO")
        )
        
        # 17. Indicador de entrega de alto valor
        # Fundamento: Identifica entregas que requieren atención especial
        # por su valor económico (top 20% aproximado)
        df = df.withColumn(
            "es_alto_valor",
            F.when(F.col("precio_total") > 1000, True).otherwise(False)
        )
        
        # 18. Código de región (primeros 2 caracteres de ruta como proxy)
        # Fundamento: Permite agrupar por zona geográfica para análisis regional
        df = df.withColumn(
            "codigo_region",
            F.when(F.col("ruta").isNotNull(), F.substring(F.col("ruta"), 1, 2))
             .otherwise("ND")
        )
        
        # Eliminar columna temporal
        df = df.drop("fecha_date")
        
        return df
    
    def standardize_columns(self, df: DataFrame) -> DataFrame:
        """
        Renombra columnas al estándar snake_case definido en configuración.
        
        Args:
            df: DataFrame transformado
            
        Returns:
            DataFrame con columnas renombradas
        """
        logger.info("Estandarizando nombres de columnas...")
        
        column_mapping = dict(self.config.output_schema.column_mapping)
        
        for old_name, new_name in column_mapping.items():
            if old_name in df.columns:
                df = df.withColumnRenamed(old_name, new_name)
        
        # Seleccionar y ordenar columnas finales
        final_columns = [
            # Identificadores geográficos
            "codigo_pais",
            "nombre_pais",
            "codigo_region",
            
            # Identificadores temporales
            "fecha_proceso",
            "anio_proceso",
            "trimestre",
            "mes_proceso",
            "semana_del_anio",
            "dia_proceso",
            "dia_semana",
            "nombre_dia_semana",
            "periodo_mes",
            
            # Identificadores de transporte
            "id_transporte",
            "id_ruta",
            
            # Producto
            "codigo_material",
            
            # Tipo de entrega
            "codigo_tipo_entrega",
            "categoria_entrega",
            "es_entrega_rutina",
            "es_entrega_bonificacion",
            
            # Cantidades
            "cantidad_original",
            "unidad_original",
            "cantidad_unidades",
            "rango_volumen",
            
            # Precios
            "precio_unitario",
            "precio_por_unidad",
            "precio_total",
            "es_bonificacion_gratuita",
            "es_alto_valor",
            
            # Metadata
            "fecha_procesamiento_etl"
        ]
        
        # Filtrar solo columnas existentes
        existing_columns = [col for col in final_columns if col in df.columns]
        df = df.select(existing_columns)
        
        return df
    
    # =========================================================================
    # CARGA
    # =========================================================================
    
    def load(self, df: DataFrame) -> dict:
        """
        Guarda el DataFrame particionado por fecha_proceso.
        
        Args:
            df: DataFrame final a guardar
            
        Returns:
            Diccionario con información de las particiones creadas
        """
        import os
        from pathlib import Path
        
        output_base = self.config.paths.output_base
        
        # Obtener fechas únicas para el reporte
        dates = df.select("fecha_proceso").distinct().collect()
        unique_dates = [row["fecha_proceso"] for row in dates]
        
        logger.info(f"Guardando datos en: {output_base}")
        logger.info(f"Particiones a crear: {unique_dates}")
        
        # Crear directorio base si no existe
        Path(output_base).mkdir(parents=True, exist_ok=True)
        
        # Guardar CSV por cada fecha (compatible con Windows sin Hadoop)
        for date in unique_dates:
            date_df = df.filter(F.col("fecha_proceso") == date)
            csv_path = os.path.join(output_base, f"fecha_proceso={date}")
            
            # Crear directorio de partición
            Path(csv_path).mkdir(parents=True, exist_ok=True)
            
            # Convertir a Pandas y guardar como CSV (evita problemas de Hadoop en Windows)
            pandas_df = date_df.toPandas()
            output_file = os.path.join(csv_path, "data.csv")
            pandas_df.to_csv(output_file, index=False, encoding='utf-8')
            
            logger.info(f"Partición guardada: {csv_path} ({len(pandas_df)} registros)")
        
        partition_info = {
            "output_path": output_base,
            "partitions_created": unique_dates,
            "total_partitions": len(unique_dates),
            "format": "csv"
        }
        
        logger.info(f"Datos guardados exitosamente. Particiones: {len(unique_dates)}")
        
        return partition_info
    
    # =========================================================================
    # EJECUCIÓN PRINCIPAL
    # =========================================================================
    
    def run(self) -> dict:
        """
        Ejecuta el pipeline ETL completo.
        
        Returns:
            Diccionario con métricas de ejecución
        """
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info(f"Iniciando ETL - Entorno: {self.config.environment}")
        logger.info("=" * 60)
        
        try:
            # 1. Extracción
            df_raw = self.extract()
            
            # 2. Calidad de datos
            df_clean, quality_metrics = self.apply_data_quality(df_raw)
            
            # 3. Filtros
            df_filtered = self.apply_filters(df_clean)
            
            # 4. Transformaciones
            df_transformed = self.transform(df_filtered)
            
            # 5. Estandarización de columnas
            df_final = self.standardize_columns(df_transformed)
            
            # 6. Carga
            partition_info = self.load(df_final)
            
            end_time = datetime.now()
            execution_time = (end_time - start_time).total_seconds()
            
            # Métricas finales
            execution_metrics = {
                "status": "SUCCESS",
                "environment": self.config.environment,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "execution_time_seconds": execution_time,
                "filters_applied": {
                    "start_date": self.config.filters.start_date,
                    "end_date": self.config.filters.end_date,
                    "country": self.config.filters.country
                },
                "quality_metrics": quality_metrics,
                "output_info": partition_info,
                "final_record_count": df_final.count()
            }
            
            logger.info("=" * 60)
            logger.info(f"ETL completado exitosamente en {execution_time:.2f} segundos")
            logger.info(f"Registros procesados: {execution_metrics['final_record_count']}")
            logger.info("=" * 60)
            
            return execution_metrics
            
        except Exception as e:
            logger.error(f"Error en ETL: {str(e)}")
            raise
        
        finally:
            self.spark.stop()
            logger.info("SparkSession cerrada")


def create_etl(config: DictConfig) -> EntregasETL:
    """
    Factory function para crear instancia de ETL.
    
    Args:
        config: Configuración de OmegaConf
        
    Returns:
        Instancia de EntregasETL
    """
    return EntregasETL(config)
