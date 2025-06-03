import pandas as pd
import numpy as np
from datetime import datetime
from src import config

class DataTransformer:
    def __init__(self):
        """
        Inizializza il trasformatore di dati.
        """
        self.current_time = datetime.now()
        self.MAX_WORKING_AGE = 70  # Definizione età lavorativa massima
        self.MIN_WORKING_AGE = 16  # Definizione età lavorativa minima

    def transform_data(self, df):
        """
        Trasforma e pulisce i dati, includendo nuove validazioni.
        Args:
            df (pd.DataFrame): DataFrame da trasformare.
        Returns:
            pd.DataFrame: DataFrame trasformato.
            dict: Statistiche sulla trasformazione.
        Raises:
            ValueError: Se il DataFrame è None.
        """
        print("\nTrasformando i dati...")
        if df is None:
            raise ValueError("Nessun dato da trasformare. Esegui prima l'estrazione.")

        # Copia per evitare warning di SettingWithCopyWarning
        df_transformed = df.copy()

        # Inizializzazione contatori per le statistiche di validazione
        negative_salaries_handled = 0
        future_hire_dates_handled = 0
        invalid_ages_removed = 0
        invalid_names_removed = 0
        invalid_surnames_removed = 0
        invalid_departments_removed = 0
        
        initial_rows = len(df_transformed)

        # 0. Validazioni preliminari
        # 0.1 Validazione Stipendi Negativi
        if config.SALARY_COLUMN in df_transformed.columns:
            negative_salary_mask = df_transformed[config.SALARY_COLUMN] < 0
            negative_salaries_handled = negative_salary_mask.sum()
            if negative_salaries_handled > 0:
                print(f"Trovati {negative_salaries_handled} record con stipendi negativi. Saranno convertiti in NaN.")
                df_transformed.loc[negative_salary_mask, config.SALARY_COLUMN] = np.nan
        
        # 0.2 Validazione Date di Assunzione Future
        if config.HIRE_DATE_COLUMN in df_transformed.columns:
            # Converti prima in datetime, coercing errors
            df_transformed[config.HIRE_DATE_COLUMN] = pd.to_datetime(df_transformed[config.HIRE_DATE_COLUMN], errors='coerce')
            future_hire_date_mask = df_transformed[config.HIRE_DATE_COLUMN] > self.current_time
            future_hire_dates_handled = future_hire_date_mask.sum()
            if future_hire_dates_handled > 0:
                print(f"Trovate {future_hire_dates_handled} date di assunzione future. Saranno convertite in NaT.")
                df_transformed.loc[future_hire_date_mask, config.HIRE_DATE_COLUMN] = pd.NaT

        # 0.3 Validazione Età
        if config.AGE_COLUMN in df_transformed.columns:
            # Assicurati che l'età sia numerica, gli errori diventano NaN
            df_transformed[config.AGE_COLUMN] = pd.to_numeric(df_transformed[config.AGE_COLUMN], errors='coerce')
            invalid_age_mask = (df_transformed[config.AGE_COLUMN] < self.MIN_WORKING_AGE) | \
                               (df_transformed[config.AGE_COLUMN] > self.MAX_WORKING_AGE) | \
                               (df_transformed[config.AGE_COLUMN].isna()) # Rimuovi anche le età diventate NaN per coercizione
            
            rows_before_age_validation = len(df_transformed)
            df_transformed = df_transformed[~invalid_age_mask]
            invalid_ages_removed = rows_before_age_validation - len(df_transformed)
            if invalid_ages_removed > 0:
                print(f"Rimossi {invalid_ages_removed} record con età non valide (min: {self.MIN_WORKING_AGE}, max: {self.MAX_WORKING_AGE}, o non numeriche).")

        # 0.4 Validazione Nome, Cognome, Reparto (Stringhe non vuote)
        critical_string_columns = {
            'nome': 'invalid_names_removed',
            'cognome': 'invalid_surnames_removed',
            config.DEPARTMENT_COLUMN: 'invalid_departments_removed'
        }

        for col, stat_key in critical_string_columns.items():
            if col in df_transformed.columns:
                rows_before_col_validation = len(df_transformed)
                # Maschera per valori non stringa, stringhe vuote o stringhe di soli spazi
                invalid_mask = (~df_transformed[col].apply(lambda x: isinstance(x, str))) | \
                               (df_transformed[col].str.strip() == '') | \
                               (df_transformed[col].isna())
                
                df_transformed = df_transformed[~invalid_mask]
                removed_count = rows_before_col_validation - len(df_transformed)
                
                if stat_key == 'invalid_names_removed':
                    invalid_names_removed = removed_count
                elif stat_key == 'invalid_surnames_removed':
                    invalid_surnames_removed = removed_count
                elif stat_key == 'invalid_departments_removed':
                    invalid_departments_removed = removed_count
                
                if removed_count > 0:
                    print(f"Rimossi {removed_count} record con '{col}' non valido.")
            else:
                print(f"Attenzione: la colonna '{col}' non è presente nel DataFrame per la validazione.")


        # 1. Rimuovere i duplicati
        # Considera tutte le colonne tranne 'id' per i duplicati
        # Se l'id non è l'identificativo univoco e altre colonne possono definire un duplicato
        columns_to_check_duplicates = [col for col in df_transformed.columns if col != 'id']
        if not columns_to_check_duplicates: # Se c'è solo la colonna id o nessuna colonna
             columns_to_check_duplicates = df_transformed.columns.tolist()
        
        size_before_duplicates = len(df_transformed)
        df_transformed.drop_duplicates(subset=columns_to_check_duplicates, inplace=True)
        duplicates_removed = size_before_duplicates - len(df_transformed)
        print(f"Rimossi {duplicates_removed} record duplicati.")

        # 2. Gestire i valori mancanti
        # Nota: i valori mancanti creati dalle validazioni (stipendio, data_assunzione) verranno gestiti qui
        missing_salary_count_after_validation = df_transformed[config.SALARY_COLUMN].isna().sum()
        missing_hire_date_count_after_validation = df_transformed[config.HIRE_DATE_COLUMN].isna().sum()

        # Calcolo dello stipendio medio per reparto
        avg_salary_by_department = df_transformed.groupby(config.DEPARTMENT_COLUMN)[config.SALARY_COLUMN].transform('mean')
        global_avg_salary = df_transformed[config.SALARY_COLUMN].mean() 

        df_transformed[config.SALARY_COLUMN] = df_transformed[config.SALARY_COLUMN].fillna(avg_salary_by_department)
        # Se avg_salary_by_department è NaN (e.g. un reparto ha solo NaN come stipendi), usa la media globale
        df_transformed[config.SALARY_COLUMN] = df_transformed[config.SALARY_COLUMN].fillna(global_avg_salary)
        
        # Gestione data assunzione mancante
        # Assicurati che la colonna data_assunzione sia datetime (dovrebbe esserlo già dal check precedente)
        df_transformed[config.HIRE_DATE_COLUMN] = pd.to_datetime(df_transformed[config.HIRE_DATE_COLUMN], errors='coerce')
        
        median_hire_date_by_department_transform = df_transformed.groupby(config.DEPARTMENT_COLUMN)[config.HIRE_DATE_COLUMN].transform('median')
        global_median_hire_date = df_transformed[config.HIRE_DATE_COLUMN].median()

        df_transformed[config.HIRE_DATE_COLUMN] = df_transformed[config.HIRE_DATE_COLUMN].fillna(median_hire_date_by_department_transform)
        df_transformed[config.HIRE_DATE_COLUMN] = df_transformed[config.HIRE_DATE_COLUMN].fillna(global_median_hire_date)
        
        # Ricalcola i conteggi dei valori imputati dopo l'effettiva imputazione
        imputed_salary_count = missing_salary_count_after_validation
        imputed_hire_date_count = missing_hire_date_count_after_validation

        print(f"Imputati {imputed_salary_count} valori mancanti per stipendio.")
        print(f"Imputati {imputed_hire_date_count} valori mancanti per data assunzione.")

        # 3. Convertire i tipi di dati
        # L'età è già stata validata e le righe problematiche rimosse, quindi astype(int) dovrebbe essere sicuro.
        df_transformed[config.AGE_COLUMN] = df_transformed[config.AGE_COLUMN].astype(int)
        df_transformed[config.SALARY_COLUMN] = df_transformed[config.SALARY_COLUMN].astype(float)
        df_transformed[config.HIRE_DATE_COLUMN] = pd.to_datetime(df_transformed[config.HIRE_DATE_COLUMN]) # Assicura tipo corretto

        # 4. Creare nuove colonne derivate
        df_transformed['anni_di_servizio'] = (self.current_time - df_transformed[config.HIRE_DATE_COLUMN]).dt.days / 365.25
        df_transformed['anni_di_servizio'] = df_transformed['anni_di_servizio'].round(1)
        # Gestisci eventuali anni di servizio negativi (se una data di assunzione valida ma molto recente è stata imputata con una data futura non catturata)
        df_transformed.loc[df_transformed['anni_di_servizio'] < 0, 'anni_di_servizio'] = 0

        df_transformed['stipendio_orario'] = round(df_transformed[config.SALARY_COLUMN] / (config.HOURS_PER_WEEK * config.WEEKS_PER_YEAR), 2)
        
        df_transformed['fascia_eta'] = pd.cut(
            df_transformed[config.AGE_COLUMN],
            bins=config.AGE_BINS,
            labels=config.AGE_LABELS,
            right=False 
        )
        
        df_transformed['fascia_stipendio'] = pd.cut(
            df_transformed[config.SALARY_COLUMN],
            bins=config.SALARY_BINS,
            labels=config.SALARY_LABELS,
            right=False
        )
        
        df_transformed['bonus'] = df_transformed['anni_di_servizio'].apply(
            lambda anni: 500 if anni < 2 else (1000 if anni < 5 else 2000)
        )
        
        df_transformed['anzianita'] = pd.cut(
            df_transformed['anni_di_servizio'],
            bins=config.SENIORITY_BINS,
            labels=config.SENIORITY_LABELS,
            right=False
        )
        
        mean_salary_by_dept_transform = df_transformed.groupby(config.DEPARTMENT_COLUMN)[config.SALARY_COLUMN].transform('mean')
        df_transformed['valutazione_stipendio'] = np.where(
            df_transformed[config.SALARY_COLUMN] > mean_salary_by_dept_transform * 1.1, 'Sopra Media',
            np.where(df_transformed[config.SALARY_COLUMN] < mean_salary_by_dept_transform * 0.9, 'Sotto Media', 'Nella Media')
        )

        transform_stats = {
            'initial_rows': initial_rows,
            'rows_after_validation_and_cleaning': len(df_transformed),
            'negative_salaries_handled': negative_salaries_handled,
            'future_hire_dates_handled': future_hire_dates_handled,
            'invalid_ages_removed': invalid_ages_removed,
            'invalid_names_removed': invalid_names_removed,
            'invalid_surnames_removed': invalid_surnames_removed,
            'invalid_departments_removed': invalid_departments_removed,
            'missing_stipendio_imputed_total': imputed_salary_count, # Totale imputati dopo validazione e imputazione
            'missing_data_assunzione_imputed_total': imputed_hire_date_count, # Totale imputati dopo validazione e imputazione
            'duplicati_rimossi': duplicates_removed,
            # 'stipendio_medio_per_reparto_input': avg_salary_by_department_before_imputation, # Richiederebbe calcolo separato prima
            'conteggio_per_reparto_output': df_transformed[config.DEPARTMENT_COLUMN].value_counts().to_dict()
        }
        
        print("Trasformazione completata.")
        return df_transformed, transform_stats
