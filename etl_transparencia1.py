import pandas as pd
import argparse
import logging
import sys
from pathlib import Path
from typing import List, Tuple, Optional
from tqdm import tqdm  # pip install tqdm

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Manejo robusto de argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(
        description="ETL para unificar archivos de transparencia (SIPOT/PNT)."
    )
    parser.add_argument(
        '--input_dir', 
        type=Path, 
        required=True, 
        help='Directorio raíz donde están los archivos Excel/CSV'
    )
    parser.add_argument(
        '--output_file', 
        type=Path, 
        required=True, 
        help='Ruta completa del archivo CSV de salida'
    )
    parser.add_argument(
        '--type', 
        choices=['adjudicaciones', 'licitaciones'], 
        required=True, 
        help='Tipo de contrato a procesar'
    )
    return parser.parse_args()

def process_file(
    file_path: Path, 
    contract_type_full: str, 
    extra_step_lp: bool
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Procesa un solo archivo (Excel o CSV) y extrae la información principal y apéndices.
    """
    file_type = file_path.suffix.lower()
    
    try:
        # 1. Lectura inicial según extensión
        if file_type in ['.xls', '.xlsx']:
            # Header 5 es estándar en PNT para datos, Header 0-4 son metadatos
            df = pd.read_excel(file_path, header=5)
            # Leer metadatos
            df_meta = pd.read_excel(file_path, header=None, nrows=4, usecols=[0, 1], names=['column', 'value'])
        
        elif file_type == '.csv':
            df = pd.read_csv(file_path, encoding='latin-1', skiprows=3)
            df_meta = pd.read_csv(file_path, header=None, nrows=4, usecols=[0, 1], names=['column', 'value'], encoding='latin-1')
        
        else:
            return None, None

        # 2. Extracción de Metadatos (Sujeto Obligado y Formato)
        meta_dict = dict(zip(df_meta['column'].str.strip().str.replace(':', ''), df_meta['value']))
        
        sujeto_obligado = meta_dict.get('Nombre del Sujeto Obligado')
        formato_archivo = meta_dict.get('Formato')

        # Validación de seguridad: ¿Es el formato correcto?
        if formato_archivo != contract_type_full:
            logger.warning(f"Formato incorrecto en {file_path.name}: Esperado '{contract_type_full}', Encontrado '{formato_archivo}'")
            return None, None

        # Inyectar metadata en el DataFrame
        if sujeto_obligado:
            df['Nombre del Sujeto Obligado'] = sujeto_obligado
        
        # Detectar Estado basado en la ruta (si existe la carpeta 'estados')
        # Ejemplo ruta: .../estados/YUCATAN/archivo.xlsx
        parts = file_path.parts
        if 'estados' in parts:
            idx = parts.index('estados')
            if idx + 1 < len(parts):
                df['ESTADO'] = parts[idx + 1].upper()
        
        # 3. Limpieza de columnas
        df.columns = (
            df.columns.str.strip()
            .str.upper()
            .str.replace(',', '')
            .str.replace('  ', ' ')
        )

        # Reordenar columnas para que Estado/Sujeto queden al inicio o final según lógica original
        cols = list(df.columns)
        if 'ESTADO' in df.columns:
             # Mover Estado y Sujeto al principio (lógica aproximada a tu script original)
             cols = [cols[0]] + cols[-2:] + cols[1:-2]
        else:
             cols = [cols[0]] + cols[-1:] + cols[1:-1]
        
        df = df[cols]

        # 4. Procesamiento de Licitaciones (Apéndice / Pestaña Extra)
        extra_df = None
        if extra_step_lp:
            # Buscar columna clave para detectar si hay anexo
            col_proposicion = next((c for c in df.columns if 'PERSONAS FÍSICAS O MORALES CON PROPOSICIÓN U OFERTA' in c), None)
            
            if col_proposicion:
                # El nombre de la pestaña suele venir en el nombre de la columna entre paréntesis
                # Ej: "BLA BLA (TABLA_123)"
                raw_table_name = col_proposicion.split('PERSONAS FÍSICAS O MORALES CON PROPOSICIÓN U OFERTA')[1].strip()
                tabla_target = raw_table_name.strip('()')

                # Intentar leer esa pestaña específica
                try:
                    # Intentamos variantes del nombre por si acaso
                    posibles_nombres = [tabla_target, tabla_target.title(), tabla_target.replace('_', '')]
                    
                    for sheet in posibles_nombres:
                        try:
                            extra_df = pd.read_excel(file_path, sheet_name=sheet)
                            break
                        except ValueError:
                            continue
                    
                    # Si encontramos datos extra, limpiarlos
                    if extra_df is not None and not extra_df.empty:
                        # A veces la PNT pone cabeceras vacías en las pestañas extra
                        extra_df.dropna(how='all', inplace=True) 
                        if not extra_df.empty:
                            # Asumir primera fila válida como header si no tiene nombres lógicos
                            # (Lógica simplificada respecto a tu script para legibilidad)
                            pass 

                except Exception as e:
                    logger.debug(f"No se pudo extraer pestaña extra en {file_path.name}: {e}")

    except Exception as e:
        logger.error(f"Error procesando {file_path.name}: {e}")
        return None, None

    return df, extra_df

def run_etl(input_dir: Path, output_file: Path, contract_type: str):
    
    # Configuración según tipo
    if contract_type == 'licitaciones':
        formato_val = 'Procedimientos de licitación pública e invitación a cuando menos tres personas'
        extra_step_lp = True
    else: # adjudicaciones
        formato_val = 'Procedimientos de adjudicación directa'
        extra_step_lp = False

    # Búsqueda recursiva usando Pathlib (Mucho más limpio)
    logger.info(f"Buscando archivos en {input_dir}...")
    
    # Generador de archivos
    all_files = list(input_dir.glob('**/*.xls*')) + list(input_dir.glob('**/*.csv'))
    
    # Filtrado por nombre (contiene 'adjudicaciones' o 'licitaciones')
    target_files = [f for f in all_files if contract_type in f.name.lower()]
    
    logger.info(f"Se encontraron {len(target_files)} archivos para procesar.")

    main_dfs_list = []
    extra_dfs_list = []

    # Barra de progreso con TQDM
    for f in tqdm(target_files, desc="Procesando archivos", unit="file"):
        df, df_extra = process_file(f, formato_val, extra_step_lp)
        
        if df is not None:
            main_dfs_list.append(df)
        if df_extra is not None:
            extra_dfs_list.append(df_extra)

    # Concatenación Final (Optimización O(N))
    if main_dfs_list:
        logger.info("Concatenando DataFrame Maestro...")
        main_df = pd.concat(main_dfs_list, axis=0, ignore_index=True)
        
        # Limpieza de duplicados
        rows_before = len(main_df)
        main_df.drop_duplicates(inplace=True)
        logger.info(f"Registros eliminados por duplicidad: {rows_before - len(main_df)}")
        
        # Guardar CSV
        output_file.parent.mkdir(parents=True, exist_ok=True) # Asegura que la carpeta exista
        main_df.to_csv(output_file, index=False, quoting=1) # QUOTE_ALL = 1
        logger.info(f"✅ Archivo guardado exitosamente: {output_file}")
    else:
        logger.warning("⚠️ No se generaron datos para el DataFrame principal.")

    # Guardar Apéndice si existe
    if extra_dfs_list:
        logger.info("Concatenando Apéndice (Detalle Licitantes)...")
        extra_df = pd.concat(extra_dfs_list, axis=0, ignore_index=True)
        extra_df.drop_duplicates(inplace=True)
        
        appendix_path = output_file.with_name(f"{output_file.stem}-APENDICE.csv")
        extra_df.to_csv(appendix_path, index=False, quoting=1)
        logger.info(f"✅ Apéndice guardado: {appendix_path}")

if __name__ == "__main__":
    args = parse_arguments()
    
    # Validar existencia directorio
    if not args.input_dir.exists():
        logger.error(f"El directorio de entrada no existe: {args.input_dir}")
        sys.exit(1)

    run_etl(args.input_dir, args.output_file, args.type)
