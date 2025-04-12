                                                                                      
                                       
import pandas as pd
import numpy as np
import logging
import warnings
import io                                                

                                                    
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - MERGE_LOGIC - %(message)s')
logger = logging.getLogger(__name__)

                                   
                                                           
                                                                                                             

def merge_all_data(df_spotify_clean: pd.DataFrame,
                   df_grammy_clean: pd.DataFrame,
                   df_youtube_clean: pd.DataFrame) -> pd.DataFrame:
    """
    Combina los DataFrames limpios de Spotify, Grammy y YouTube.

    Args:
        df_spotify_clean (pd.DataFrame): DataFrame limpio de Spotify.
        df_grammy_clean (pd.DataFrame): DataFrame limpio de Grammy.
        df_youtube_clean (pd.DataFrame): DataFrame limpio de YouTube Stats.

    Returns:
        pd.DataFrame: DataFrame final combinado y enriquecido.
                      Devuelve un DataFrame vacío si ocurre un error crítico
                      o si el DataFrame base (Spotify) es inválido.
    """
    logger.info("Iniciando el proceso de merge de los tres DataFrames.")

                                                  
    if not isinstance(df_spotify_clean, pd.DataFrame) or df_spotify_clean.empty:
        logger.error("El DataFrame de Spotify (base para el merge) es inválido o está vacío. Abortando merge.")
        return pd.DataFrame()
    if not isinstance(df_grammy_clean, pd.DataFrame):
        logger.warning("El DataFrame de Grammy es inválido. Se procederá sin información de Grammy.")
        df_grammy_clean = pd.DataFrame()                                        
    if not isinstance(df_youtube_clean, pd.DataFrame):
        logger.warning("El DataFrame de YouTube es inválido. Se procederá sin información de YouTube.")
        df_youtube_clean = pd.DataFrame()                    

                                                                          
    df_spotify = df_spotify_clean.copy()
    df_grammy = df_grammy_clean.copy()
    df_youtube = df_youtube_clean.copy()
    logger.info(f"Shapes iniciales - Spotify: {df_spotify.shape}, Grammy: {df_grammy.shape}, YouTube: {df_youtube.shape}")


    try:
                                                                   
        logger.info("Iniciando Merge 1: YouTube Stats -> Spotify")

        spotify_artist_col = 'artists'
        youtube_query_col = 'artist_query'

                                                        
        if df_youtube.empty or youtube_query_col not in df_youtube.columns:
            logger.warning(f"DataFrame de YouTube vacío o sin columna '{youtube_query_col}'. Continuando sin datos de YouTube.")
            spotify_enriched_yt = df_spotify                           
                                                                 
            yt_cols_expected = ['channel_id_found', 'channel_title_verified', 'subscriber_count', 'view_count', 'video_count', 'total_top10_video_likes']
            for col in yt_cols_expected:
                if col not in spotify_enriched_yt.columns:
                                                                             
                    if col in ['channel_id_found', 'channel_title_verified']:
                        spotify_enriched_yt[col] = ''
                    else:                     
                        spotify_enriched_yt[col] = 0
            logger.info("Columnas de YouTube añadidas con valores por defecto.")

        else:
                                                             
            if spotify_artist_col not in df_spotify.columns:
                logger.error(f"Columna clave '{spotify_artist_col}' no encontrada en Spotify DF. Abortando.")
                return pd.DataFrame()

            logger.info(f"Realizando left merge Spotify con YouTube usando '{spotify_artist_col}' y '{youtube_query_col}'.")
            spotify_enriched_yt = pd.merge(
                df_spotify,
                df_youtube,
                how='left',
                left_on=spotify_artist_col,
                right_on=youtube_query_col,
                suffixes=('', '_yt')                                                                   
            )
            logger.info(f"Merge 1 (YT->Spotify) completado. Filas resultantes: {len(spotify_enriched_yt)}")

                                     
                                                                                                
            yt_check_col = 'subscriber_count'                                  
            if yt_check_col in spotify_enriched_yt.columns:
                                                                                                             
                matched_rows = spotify_enriched_yt[yt_check_col].notna() & (spotify_enriched_yt[yt_check_col] != 0)
                                                                                                     
                                                                                                                                                                 
                logger.info(f"{matched_rows.sum()} / {len(spotify_enriched_yt)} filas de Spotify tuvieron coincidencia con estadísticas de YouTube (subs > 0).")
            else:
                logger.warning(f"No se pudo verificar coincidencias de YouTube: Columna '{yt_check_col}' no encontrada post-merge.")


                                                         
            fill_yt_na = {
                'channel_id_found': '', 'channel_title_verified': '',
                'subscriber_count': 0, 'view_count': 0,
                'video_count': 0, 'total_top10_video_likes': 0
            }
                                                                   
            cols_to_fill_yt = {k: v for k, v in fill_yt_na.items() if k in spotify_enriched_yt.columns}
            spotify_enriched_yt.fillna(cols_to_fill_yt, inplace=True)
            logger.info("NaNs de columnas de YouTube rellenados con valores por defecto (0 o '').")

                                                                                       
            yt_num_cols = ['subscriber_count', 'view_count', 'video_count', 'total_top10_video_likes']
            for col in yt_num_cols:
                if col in spotify_enriched_yt.columns:
                    try:
                                                                                           
                        spotify_enriched_yt[col] = spotify_enriched_yt[col].astype(float).astype('int64')
                    except Exception as e:
                         logger.warning(f"No se pudo convertir la columna YT '{col}' a int64: {e}")

                                                                                 
            if youtube_query_col in spotify_enriched_yt.columns and youtube_query_col != spotify_artist_col:
                 if f"{youtube_query_col}_yt" not in spotify_enriched_yt.columns:                                         
                    spotify_enriched_yt.drop(columns=[youtube_query_col], inplace=True)
                    logger.info(f"Columna '{youtube_query_col}' eliminada después del merge YT->Spotify.")


        logger.debug("Merge 1: DataFrame spotify_enriched_yt info:")
        buffer = io.StringIO()
        spotify_enriched_yt.info(buf=buffer)
        logger.debug(buffer.getvalue())


                                                                          
        logger.info("Iniciando Merge 2: Grammy Info -> Spotify Enriched")

        if df_grammy.empty:
            logger.warning("DataFrame de Grammy vacío. Continuando sin información de Grammy.")
            df_final_merged = spotify_enriched_yt.copy()
                                                                      
            df_final_merged['grammy_year'] = pd.NA                                 
            df_final_merged['grammy_category'] = ''
            df_final_merged['grammy_winner'] = False
        else:
                                                        
            cols_grammy_select = ['artist', 'nominee', 'year', 'category', 'winner']
                                                
            actual_grammy_cols = [c for c in cols_grammy_select if c in df_grammy.columns]
            if not actual_grammy_cols:
                 logger.error("No hay columnas válidas de Grammy para mergear. Continuando sin info de Grammy.")
                 df_final_merged = spotify_enriched_yt.copy()
                 df_final_merged['grammy_year'] = pd.NA
                 df_final_merged['grammy_category'] = ''
                 df_final_merged['grammy_winner'] = False
            else:
                grammy_info_to_merge = df_grammy[actual_grammy_cols].copy()
                                                                             
                rename_map_grammy = {
                    'artist': 'grammy_artist',                             
                    'nominee': 'grammy_nominee',                              
                    'year': 'grammy_year',
                    'category': 'grammy_category',
                    'winner': 'grammy_winner'
                }
                                                         
                actual_rename_map = {k: v for k, v in rename_map_grammy.items() if k in grammy_info_to_merge.columns}
                grammy_info_to_merge.rename(columns=actual_rename_map, inplace=True)
                logger.info(f"Columnas seleccionadas/renombradas de Grammy para merge: {list(grammy_info_to_merge.columns)}")

                                                                                     
                spotify_merge_keys = ['artists', 'track_name']
                                                         
                grammy_merge_keys = ['grammy_artist', 'grammy_nominee']

                                                  
                keys_ok = True
                if not all(k in spotify_enriched_yt.columns for k in spotify_merge_keys):
                     logger.error(f"Faltan columnas clave {spotify_merge_keys} en Spotify DF. Abortando merge Grammy.")
                     keys_ok = False
                if not all(k in grammy_info_to_merge.columns for k in grammy_merge_keys):
                    logger.error(f"Faltan columnas clave {grammy_merge_keys} en Grammy DF (después de renombrar). Abortando merge Grammy.")
                    keys_ok = False

                if not keys_ok:
                    df_final_merged = spotify_enriched_yt.copy()
                    df_final_merged['grammy_year'] = pd.NA
                    df_final_merged['grammy_category'] = ''
                    df_final_merged['grammy_winner'] = False
                else:
                    logger.info(f"Realizando left merge Spotify_YT con Grammy usando {spotify_merge_keys} y {grammy_merge_keys}.")
                                       
                    df_final_merged = pd.merge(
                        spotify_enriched_yt,
                        grammy_info_to_merge,
                        how='left',
                        left_on=spotify_merge_keys,
                        right_on=grammy_merge_keys,
                        suffixes=('', '_grammy')                      
                    )
                    logger.info(f"Merge 2 (Grammy->Spotify_YT) completado. Filas resultantes: {len(df_final_merged)}")

                                                                                         
                    if 'grammy_year' in df_final_merged.columns:
                        matched_rows_final = df_final_merged['grammy_year'].notna().sum()
                        logger.info(f"{matched_rows_final} / {len(df_final_merged)} filas obtuvieron información de Grammy coincidente.")
                    else:
                        logger.warning("No se pudo verificar coincidencias de Grammy: Columna 'grammy_year' no encontrada post-merge.")


                                                                          
                    if 'grammy_winner' in df_final_merged.columns:
                        df_final_merged['grammy_winner'] = df_final_merged['grammy_winner'].fillna(False).astype(bool)
                        logger.info("NaNs en 'grammy_winner' rellenados con False.")
                    if 'grammy_category' in df_final_merged.columns:
                        df_final_merged['grammy_category'] = df_final_merged['grammy_category'].fillna('')
                    if 'grammy_year' in df_final_merged.columns:
                                                                           
                        df_final_merged['grammy_year'] = pd.to_numeric(df_final_merged['grammy_year'], errors='coerce').astype('Int64')
                        logger.info("Columna 'grammy_year' convertida a Int64 (permite nulos).")

                                                                               
                    cols_to_drop_grammy = ['grammy_artist', 'grammy_nominee']
                    cols_present_to_drop = [c for c in cols_to_drop_grammy if c in df_final_merged.columns]
                    if cols_present_to_drop:
                         df_final_merged.drop(columns=cols_present_to_drop, inplace=True)
                         logger.info(f"Columnas de match de Grammy {cols_present_to_drop} eliminadas.")


                                                                                        
                    spotify_id_col = 'track_id'
                    if spotify_id_col in df_final_merged.columns:
                        duplicates_final = df_final_merged.duplicated(subset=[spotify_id_col], keep=False)
                        num_duplicates = duplicates_final.sum()
                        if num_duplicates > 0:
                            logger.warning(f"Se detectaron {num_duplicates} filas que comparten el mismo '{spotify_id_col}' después del merge final. "
                                           f"({len(df_final_merged)} filas totales). Esto puede indicar múltiples nominaciones/premios para una misma pista.")
                                                                         
                                                                                                                         
                            logger.info("Aplicando estrategia de deduplicación: Manteniendo la entrada con 'grammy_year' más reciente (o la primera si no hay año).")
                                                                                                                            
                            df_final_merged.sort_values(by=[spotify_id_col, 'grammy_year'], ascending=[True, False], na_position='last', inplace=True)
                                                                                           
                            rows_before_dedup = len(df_final_merged)
                            df_final_merged.drop_duplicates(subset=[spotify_id_col], keep='first', inplace=True)
                            rows_after_dedup = len(df_final_merged)
                            logger.info(f"Deduplicación por '{spotify_id_col}' completada. Filas eliminadas: {rows_before_dedup - rows_after_dedup}. Filas finales: {rows_after_dedup}")
                                                                        
                        else:
                            logger.info(f"No se encontraron duplicados por '{spotify_id_col}' después del merge final.")
                    else:
                        logger.warning(f"No se pudo verificar duplicados finales: Columna '{spotify_id_col}' no encontrada.")

                                          
        logger.info("Proceso de merge finalizado.")
        logger.info("Información del DataFrame final mergeado:")
        buffer = io.StringIO()
        df_final_merged.info(buf=buffer)
        logger.info(buffer.getvalue())


    except Exception as e:
        logger.error(f"Error inesperado durante el proceso de merge: {e}", exc_info=True)
        return pd.DataFrame()                                            

    return df_final_merged


                                                    
if __name__ == '__main__':
    logger.info("Ejecutando transform_merge.py como script independiente para pruebas...")

                                                                        
    spotify_data_test = {
        'track_id': ['t1', 't2', 't3', 't4', 't5'],
        'artists': ['Artist A', 'Artist B', 'Artist C;Artist D', 'Artist A', 'Artist E'],
        'track_name': ['Song Alpha', 'Song Beta', 'Song Gamma', 'Song Other', 'Song Delta'],
        'popularity': [80, 70, 60, 75, 50]                                                   
    }
    spotify_df_test = pd.DataFrame(spotify_data_test)
                                                  
    for col in ['track_id', 'artists', 'track_name']: spotify_df_test[col] = spotify_df_test[col].astype('string')
    spotify_df_test['popularity'] = spotify_df_test['popularity'].astype('int64')


    grammy_data_test = {
        'artist': ['Artist A', 'Artist C;Artist D', 'Artist A', 'Artist Z'],
        'nominee': ['Song Alpha', 'Song Gamma', 'Song Alpha', 'Some Song'],                                             
        'year': [2020, 2021, 2022, 2021],
        'category': ['Best Song', 'Best Collab', 'Record Of Year', 'Best New'],
        'winner': [True, False, False, False]
    }
    grammy_df_test = pd.DataFrame(grammy_data_test)
                                                  
    grammy_df_test['winner'] = grammy_df_test['winner'].astype(bool)
    grammy_df_test['year'] = grammy_df_test['year'].astype('Int64')                                                 
    for col in ['artist', 'nominee', 'category']: grammy_df_test[col] = grammy_df_test[col].astype('string')


    youtube_data_test = {
        'artist_query': ['Artist A', 'Artist B', 'Artist C;Artist D', 'Artist E'],                          
        'channel_id_found': ['chA', 'chB', 'chCD', 'chE'],
        'channel_title_verified': ['Artist A Verified', 'Artist B Channel', 'C and D Music', 'Artist E Official'],
        'subscriber_count': [1000, 500, 2000, 1500],
        'view_count': [100000, 20000, 50000, 120000],
        'video_count': [10, 5, 20, 15],
        'total_top10_video_likes': [5000, 1000, 3000, 6000]
    }
    youtube_df_test = pd.DataFrame(youtube_data_test)
                                                  
    for col in ['artist_query', 'channel_id_found', 'channel_title_verified']: youtube_df_test[col] = youtube_df_test[col].astype('string')
    for col in ['subscriber_count', 'view_count', 'video_count', 'total_top10_video_likes']: youtube_df_test[col] = youtube_df_test[col].astype('int64')


    logger.info("DataFrames de prueba creados.")

                                          
    df_merged_result = merge_all_data(spotify_df_test, grammy_df_test, youtube_df_test)

                                
    if not df_merged_result.empty:
        logger.info(f"Merge standalone completado. Filas resultantes: {len(df_merged_result)}")
        print("\nDataFrame Mergeado Resultante (Prueba):")
                                                                          
        try:
            from IPython.display import display
            display(df_merged_result)
        except ImportError:
            print(df_merged_result.to_string())

        print("\nInfo del DataFrame Mergeado:")
        df_merged_result.info()
    else:
        logger.error("El merge standalone falló o devolvió un DataFrame vacío.")
                                       