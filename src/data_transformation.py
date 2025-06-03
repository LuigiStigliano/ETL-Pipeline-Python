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

    def transform_data(self, df):
        """
        Trasforma e pulisce i dati.
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

        # 1. Rimuovere i duplicati
        initial_size = len(df_transformed)
        # Considera tutte le colonne tranne 'id' per i duplicati
        # Se l'id non è l'identificativo univoco e altre colonne possono definire un duplicato
        columns_to_check_duplicates = [col for col in df_transformed.columns if col != 'id']
        if not columns_to_check_duplicates: # Se c'è solo la colonna id o nessuna colonna
             columns_to_check_duplicates = df_transformed.columns.tolist()

        df_transformed.drop_duplicates(subset=columns_to_check_duplicates, inplace=True)
        duplicates_removed = initial_size - len(df_transformed)
        print(f"Rimossi {duplicates_removed} record duplicati.")

        # 2. Gestire i valori mancanti
        missing_salary_count = df_transformed[config.SALARY_COLUMN].isna().sum()
        missing_hire_date_count = df_transformed[config.HIRE_DATE_COLUMN].isna().sum()

        # Calcolo dello stipendio medio per reparto
        # Usiamo .mean() che ignora i NaN di default per il calcolo della media
        avg_salary_by_department = df_transformed.groupby(config.DEPARTMENT_COLUMN)[config.SALARY_COLUMN].mean().to_dict()
        global_avg_salary = df_transformed[config.SALARY_COLUMN].mean() # Media globale per reparti non trovati o se tutti i valori sono NaN

        # Sostituiamo gli stipendi mancanti con la media del reparto o globale se necessario
        for i, row in df_transformed[df_transformed[config.SALARY_COLUMN].isna()].iterrows():
            department = row[config.DEPARTMENT_COLUMN]
            df_transformed.at[i, config.SALARY_COLUMN] = avg_salary_by_department.get(department, global_avg_salary)
        
        df_transformed[config.HIRE_DATE_COLUMN] = pd.to_datetime(df_transformed[config.HIRE_DATE_COLUMN], errors='coerce')
        median_hire_date_by_department = df_transformed.groupby(config.DEPARTMENT_COLUMN)[config.HIRE_DATE_COLUMN].median().to_dict()
        global_median_hire_date = df_transformed[config.HIRE_DATE_COLUMN].median()

        for i, row in df_transformed[df_transformed[config.HIRE_DATE_COLUMN].isna()].iterrows():
            department = row[config.DEPARTMENT_COLUMN]
            df_transformed.at[i, config.HIRE_DATE_COLUMN] = median_hire_date_by_department.get(department, global_median_hire_date)
        
        # 3. Convertire i tipi di dati
        df_transformed[config.AGE_COLUMN] = df_transformed[config.AGE_COLUMN].astype(int)
        df_transformed[config.SALARY_COLUMN] = df_transformed[config.SALARY_COLUMN].astype(float)
        df_transformed[config.HIRE_DATE_COLUMN] = pd.to_datetime(df_transformed[config.HIRE_DATE_COLUMN])

        # 4. Creare nuove colonne derivate
        df_transformed['anni_di_servizio'] = (self.current_time - df_transformed[config.HIRE_DATE_COLUMN]).dt.days / 365.25
        df_transformed['anni_di_servizio'] = df_transformed['anni_di_servizio'].round(1)
        
        df_transformed['stipendio_orario'] = round(df_transformed[config.SALARY_COLUMN] / (config.HOURS_PER_WEEK * config.WEEKS_PER_YEAR), 2)
        
        df_transformed['fascia_eta'] = pd.cut(
            df_transformed[config.AGE_COLUMN],
            bins=config.AGE_BINS,
            labels=config.AGE_LABELS,
            right=False # [0, 30), [30, 40), ...
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
            'missing_stipendio_imputed': missing_salary_count,
            'missing_data_assunzione_imputed': missing_hire_date_count,
            'duplicati_rimossi': duplicates_removed,
            'stipendio_medio_per_reparto_input': avg_salary_by_department, # Media calcolata prima dell'imputazione
            'conteggio_per_reparto': df_transformed[config.DEPARTMENT_COLUMN].value_counts().to_dict()
        }
        
        print("Trasformazione completata.")
        return df_transformed, transform_stats