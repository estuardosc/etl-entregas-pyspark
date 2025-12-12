"""
Tests para el ETL de Entregas de Productos
==========================================
"""

import pytest
from pathlib import Path
from omegaconf import OmegaConf
from pyspark.sql import SparkSession
import sys

# Agregar src al path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from etl_entregas import EntregasETL


@pytest.fixture(scope="module")
def spark():
    """Fixture para crear SparkSession de pruebas."""
    spark = SparkSession.builder \
        .appName("ETL_Test") \
        .master("local[2]") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def test_config():
    """Fixture para configuración de pruebas."""
    config_yaml = """
    environment: test
    paths:
      input_file: data/raw/data_entrega_productos.csv
      output_base: data/processed/test
    filters:
      start_date: "20250101"
      end_date: "20250630"
      country: null
    spark:
      app_name: "ETL_Test"
      master: "local[2]"
      log_level: "ERROR"
      configs:
        spark.sql.shuffle.partitions: 2
        spark.driver.memory: "1g"
    business_rules:
      units_conversion:
        CS: 20
        ST: 1
      delivery_types:
        routine:
          - ZPRE
          - ZVE1
        bonus:
          - Z04
          - Z05
    data_quality:
      remove_null_material: true
      remove_zero_price: false
      remove_duplicates: true
      validate_country_codes: true
      valid_countries:
        - GT
        - SV
        - HN
        - EC
        - PE
        - JM
    output_schema:
      column_mapping:
        pais: codigo_pais
        fecha_proceso: fecha_proceso
        transporte: id_transporte
        ruta: id_ruta
        tipo_entrega: codigo_tipo_entrega
        material: codigo_material
        precio: precio_unitario
        cantidad: cantidad_original
        unidad: unidad_original
    country_names:
      GT: Guatemala
      SV: El Salvador
      HN: Honduras
      EC: Ecuador
      PE: Perú
      JM: Jamaica
    logging:
      level: ERROR
      format: "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    """
    return OmegaConf.create(config_yaml)


@pytest.fixture
def sample_data(spark):
    """Fixture para crear datos de prueba."""
    data = [
        ("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", 100.0, 5.0, "CS"),
        ("GT", "20250115", "T001", "R001", "Z04", "MAT002", 50.0, 10.0, "ST"),
        ("SV", "20250220", "T002", "R002", "ZVE1", "MAT003", 200.0, 3.0, "CS"),
        ("HN", "20250310", "T003", "R003", "COBR", "MAT004", 75.0, 8.0, "ST"),  # Debe ser excluido
        ("EC", "20250415", "T004", "R004", "Z05", "", 120.0, 2.0, "CS"),  # Material vacío
    ]
    columns = ["pais", "fecha_proceso", "transporte", "ruta", "tipo_entrega", 
               "material", "precio", "cantidad", "unidad"]
    return spark.createDataFrame(data, columns)


class TestDataQuality:
    """Tests para reglas de calidad de datos."""
    
    def test_filter_invalid_delivery_types(self, spark, test_config, sample_data):
        """Verifica que se filtren tipos de entrega inválidos (COBR)."""
        etl = EntregasETL(test_config)
        etl.spark = spark
        
        df_clean, metrics = etl.apply_data_quality(sample_data)
        
        # COBR debe ser excluido
        cobr_count = df_clean.filter(df_clean.tipo_entrega == "COBR").count()
        assert cobr_count == 0, "Los registros COBR no fueron filtrados"
    
    def test_remove_null_material(self, spark, test_config, sample_data):
        """Verifica que se eliminen registros con material nulo."""
        etl = EntregasETL(test_config)
        etl.spark = spark
        
        df_clean, metrics = etl.apply_data_quality(sample_data)
        
        # Verificar que materiales vacíos fueron eliminados
        empty_material = df_clean.filter(
            (df_clean.material.isNull()) | (df_clean.material == "")
        ).count()
        assert empty_material == 0, "Los registros con material vacío no fueron eliminados"


class TestTransformations:
    """Tests para transformaciones de datos."""
    
    def test_unit_conversion_cs(self, spark, test_config):
        """Verifica conversión de CS (cajas) a unidades."""
        etl = EntregasETL(test_config)
        etl.spark = spark
        
        # Crear datos de prueba con CS
        data = [("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", 100.0, 5.0, "CS")]
        columns = ["pais", "fecha_proceso", "transporte", "ruta", "tipo_entrega", 
                   "material", "precio", "cantidad", "unidad"]
        df = spark.createDataFrame(data, columns)
        
        df_transformed = etl.transform(df)
        
        # 5 cajas * 20 = 100 unidades
        cantidad_unidades = df_transformed.select("cantidad_unidades").collect()[0][0]
        assert cantidad_unidades == 100.0, f"Conversión incorrecta: esperado 100, obtenido {cantidad_unidades}"
    
    def test_unit_conversion_st(self, spark, test_config):
        """Verifica que ST se mantenga igual."""
        etl = EntregasETL(test_config)
        etl.spark = spark
        
        data = [("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", 100.0, 10.0, "ST")]
        columns = ["pais", "fecha_proceso", "transporte", "ruta", "tipo_entrega", 
                   "material", "precio", "cantidad", "unidad"]
        df = spark.createDataFrame(data, columns)
        
        df_transformed = etl.transform(df)
        
        # ST = 10 unidades
        cantidad_unidades = df_transformed.select("cantidad_unidades").collect()[0][0]
        assert cantidad_unidades == 10.0, f"Conversión incorrecta: esperado 10, obtenido {cantidad_unidades}"
    
    def test_delivery_category_routine(self, spark, test_config):
        """Verifica clasificación de entrega rutina."""
        etl = EntregasETL(test_config)
        etl.spark = spark
        
        data = [("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", 100.0, 5.0, "CS")]
        columns = ["pais", "fecha_proceso", "transporte", "ruta", "tipo_entrega", 
                   "material", "precio", "cantidad", "unidad"]
        df = spark.createDataFrame(data, columns)
        
        df_transformed = etl.transform(df)
        
        categoria = df_transformed.select("categoria_entrega").collect()[0][0]
        es_rutina = df_transformed.select("es_entrega_rutina").collect()[0][0]
        
        assert categoria == "RUTINA", f"Categoría incorrecta: {categoria}"
        assert es_rutina == True, "Flag es_entrega_rutina incorrecto"
    
    def test_delivery_category_bonus(self, spark, test_config):
        """Verifica clasificación de entrega bonificación."""
        etl = EntregasETL(test_config)
        etl.spark = spark
        
        data = [("GT", "20250115", "T001", "R001", "Z04", "MAT001", 100.0, 5.0, "CS")]
        columns = ["pais", "fecha_proceso", "transporte", "ruta", "tipo_entrega", 
                   "material", "precio", "cantidad", "unidad"]
        df = spark.createDataFrame(data, columns)
        
        df_transformed = etl.transform(df)
        
        categoria = df_transformed.select("categoria_entrega").collect()[0][0]
        es_bonificacion = df_transformed.select("es_entrega_bonificacion").collect()[0][0]
        
        assert categoria == "BONIFICACION", f"Categoría incorrecta: {categoria}"
        assert es_bonificacion == True, "Flag es_entrega_bonificacion incorrecto"


class TestFilters:
    """Tests para filtros de datos."""
    
    def test_date_filter(self, spark, test_config):
        """Verifica filtro por rango de fechas."""
        etl = EntregasETL(test_config)
        etl.spark = spark
        
        # Modificar config para filtrar solo enero
        test_config.filters.start_date = "20250101"
        test_config.filters.end_date = "20250131"
        
        data = [
            ("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", 100.0, 5.0, "CS"),  # Incluido
            ("GT", "20250215", "T002", "R002", "ZPRE", "MAT002", 100.0, 5.0, "CS"),  # Excluido
        ]
        columns = ["pais", "fecha_proceso", "transporte", "ruta", "tipo_entrega", 
                   "material", "precio", "cantidad", "unidad"]
        df = spark.createDataFrame(data, columns)
        
        df_filtered = etl.apply_filters(df)
        
        count = df_filtered.count()
        assert count == 1, f"Filtro de fechas incorrecto: esperado 1, obtenido {count}"
    
    def test_country_filter(self, spark, test_config):
        """Verifica filtro por país."""
        etl = EntregasETL(test_config)
        etl.spark = spark
        
        # Modificar config para filtrar solo GT
        test_config.filters.country = "GT"
        
        data = [
            ("GT", "20250115", "T001", "R001", "ZPRE", "MAT001", 100.0, 5.0, "CS"),  # Incluido
            ("SV", "20250115", "T002", "R002", "ZPRE", "MAT002", 100.0, 5.0, "CS"),  # Excluido
        ]
        columns = ["pais", "fecha_proceso", "transporte", "ruta", "tipo_entrega", 
                   "material", "precio", "cantidad", "unidad"]
        df = spark.createDataFrame(data, columns)
        
        df_filtered = etl.apply_filters(df)
        
        count = df_filtered.count()
        assert count == 1, f"Filtro de país incorrecto: esperado 1, obtenido {count}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
