#!/usr/bin/env python3
"""
ETL Entregas de Productos - Script Principal
=============================================
Entrada para la ejecución del pipeline ETL.

Uso:
    # Ejecución con configuración por defecto
    python main.py
    
    # Especificar entorno
    python main.py --env develop
    python main.py --env qa
    python main.py --env main
    
    # Sobreescribir parámetros via CLI
    python main.py filters.start_date=20250101 filters.end_date=20250331
    
    # Filtrar por país específico
    python main.py filters.country=GT
    
    # Combinación de parámetros
    python main.py --env qa filters.start_date=20250201 filters.end_date=20250228 filters.country=SV

Author: Estuardo SC
Version: 1.2
"""

import argparse
import sys
import json
from pathlib import Path
from omegaconf import OmegaConf, DictConfig
from typing import Optional

# Agregar src al path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from src.etl_entregas import EntregasETL, create_etl


def load_config(env: str = "develop", overrides: Optional[list] = None) -> DictConfig:
    """
    Carga la configuración base y la específica del entorno.
    
    Args:
        env: Entorno a usar (develop, qa, main)
        overrides: Lista de overrides en formato "key=value"
        
    Returns:
        Configuración combinada de OmegaConf
    """
    config_dir = Path(__file__).parent / "config"
    
    # Cargar configuración base
    base_config_path = config_dir / "config.yaml"
    if not base_config_path.exists():
        raise FileNotFoundError(f"Archivo de configuración no encontrado: {base_config_path}")
    
    base_config = OmegaConf.load(base_config_path)
    
    # Cargar configuración del entorno
    env_config_path = config_dir / f"config_{env}.yaml"
    if env_config_path.exists():
        env_config = OmegaConf.load(env_config_path)
        # Merge: env_config sobreescribe base_config
        config = OmegaConf.merge(base_config, env_config)
    else:
        print(f"Advertencia: No se encontró config_{env}.yaml, usando configuración base")
        config = base_config
    
    # Aplicar overrides de CLI
    if overrides:
        cli_config = OmegaConf.from_dotlist(overrides)
        config = OmegaConf.merge(config, cli_config)
    
    return config


def parse_arguments():
    """
    Parsea los argumentos de línea de comandos.
    
    Returns:
        Namespace con argumentos parseados y lista de overrides
    """
    parser = argparse.ArgumentParser(
        description="ETL Entregas de Productos - Procesamiento parametrizable",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos de uso:
  %(prog)s --env develop
  %(prog)s --env qa filters.start_date=20250101 filters.end_date=20250331
  %(prog)s filters.country=GT
  %(prog)s --env main filters.start_date=20250201 filters.end_date=20250228 filters.country=SV
        """
    )
    
    parser.add_argument(
        "--env", 
        type=str, 
        default="develop",
        choices=["develop", "qa", "main"],
        help="Entorno de ejecución (default: develop)"
    )
    
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Muestra la configuración sin ejecutar el ETL"
    )
    
    parser.add_argument(
        "--show-config",
        action="store_true",
        help="Muestra la configuración completa"
    )
    
    # Capturar argumentos adicionales como overrides de OmegaConf
    args, overrides = parser.parse_known_args()
    
    return args, overrides


def validate_date_range(config: DictConfig) -> bool:
    """
    Valida que el rango de fechas sea correcto.
    
    Args:
        config: Configuración a validar
        
    Returns:
        True si el rango es válido
    """
    start = str(config.filters.start_date)
    end = str(config.filters.end_date)
    
    # Validar formato
    if len(start) != 8 or len(end) != 8:
        print(f"Error: Las fechas deben tener formato YYYYMMDD")
        return False
    
    # Validar que start <= end
    if start > end:
        print(f"Error: La fecha de inicio ({start}) debe ser menor o igual a la fecha de fin ({end})")
        return False
    
    return True


def print_banner():
    """Imprime el banner de inicio."""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║           ETL ENTREGAS DE PRODUCTOS - PySpark                ║
    ║                     v1.0.0                                   ║
    ╠══════════════════════════════════════════════════════════════╣
    ║  Procesamiento parametrizable de entregas con OmegaConf      ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)


def print_execution_summary(metrics: dict):
    """
    Imprime un resumen de la ejecución.
    
    Args:
        metrics: Métricas de ejecución del ETL
    """
    print("\n" + "=" * 60)
    print("RESUMEN DE EJECUCIÓN")
    print("=" * 60)
    print(f"Estado: {metrics['status']}")
    print(f"Entorno: {metrics['environment']}")
    print(f"Tiempo de ejecución: {metrics['execution_time_seconds']:.2f} segundos")
    print(f"\nFiltros aplicados:")
    print(f"  - Fecha inicio: {metrics['filters_applied']['start_date']}")
    print(f"  - Fecha fin: {metrics['filters_applied']['end_date']}")
    print(f"  - País: {metrics['filters_applied']['country'] or 'TODOS'}")
    print(f"\nMétricas de calidad:")
    for key, value in metrics['quality_metrics'].items():
        print(f"  - {key}: {value}")
    print(f"\nSalida:")
    print(f"  - Ruta: {metrics['output_info']['output_path']}")
    print(f"  - Particiones creadas: {metrics['output_info']['total_partitions']}")
    print(f"  - Fechas: {', '.join(metrics['output_info']['partitions_created'])}")
    print(f"\nRegistros procesados: {metrics['final_record_count']}")
    print("=" * 60)


def main():
    """Función principal de ejecución."""
    print_banner()
    
    # Parsear argumentos
    args, overrides = parse_arguments()
    
    print(f"Entorno seleccionado: {args.env}")
    if overrides:
        print(f"Overrides de configuración: {overrides}")
    
    try:
        # Cargar configuración
        config = load_config(env=args.env, overrides=overrides)
        
        # Mostrar configuración si se solicita
        if args.show_config:
            print("\nConfiguración actual:")
            print(OmegaConf.to_yaml(config))
            return 0
        
        # Validar rango de fechas
        if not validate_date_range(config):
            return 1
        
        # Dry run
        if args.dry_run:
            print("\n[DRY RUN] Configuración que se usaría:")
            print(f"  - Input: {config.paths.input_file}")
            print(f"  - Output: {config.paths.output_base}")
            print(f"  - Fechas: {config.filters.start_date} a {config.filters.end_date}")
            print(f"  - País: {config.filters.country or 'TODOS'}")
            return 0
        
        # Ejecutar ETL
        etl = create_etl(config)
        metrics = etl.run()
        
        # Mostrar resumen
        print_execution_summary(metrics)
        
        # Guardar métricas en JSON
        metrics_path = Path(config.paths.output_base) / "execution_metrics.json"
        metrics_path.parent.mkdir(parents=True, exist_ok=True)
        with open(metrics_path, "w") as f:
            json.dump(metrics, f, indent=2, default=str)
        print(f"\nMétricas guardadas en: {metrics_path}")
        
        return 0
        
    except Exception as e:
        print(f"\nError durante la ejecución: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
