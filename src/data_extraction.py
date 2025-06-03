import pandas as pd
from src import config

class DataExtractor:
    def __init__(self, input_path):
        """
        Inizializza l'estrattore di dati.
        Args:
            input_path (str): Percorso del file CSV di input.
        """
        self.input_path = input_path

    def extract_data(self):
        """
        Estrae i dati dal file CSV.
        Returns:
            pd.DataFrame: DataFrame contenente i dati estratti.
            pd.DataFrame: Copia del DataFrame originale.
        Raises:
            Exception: Se si verifica un errore durante l'estrazione.
        """
        print(f"Estraendo dati da {self.input_path}...")
        try:
            data = pd.read_csv(self.input_path)
            data_originale = data.copy() # Salva una copia per confronti successivi
            print(f"Estratti {len(data)} record.")

            # Mostra alcune statistiche iniziali
            print("\nStatistiche iniziali:")
            print(f"Numero di record: {len(data)}")
            print(f"Numero di colonne: {len(data.columns)}")
            if config.DEPARTMENT_COLUMN in data.columns:
                print(f"Reparti presenti: {data[config.DEPARTMENT_COLUMN].unique()}")
            if config.AGE_COLUMN in data.columns:
                print(f"Range età: {data[config.AGE_COLUMN].min()} - {data[config.AGE_COLUMN].max()}")
            if config.SALARY_COLUMN in data.columns:
                print(f"Range stipendio: {data[config.SALARY_COLUMN].min()} - {data[config.SALARY_COLUMN].max()}")

            # Verifica dei valori mancanti
            missing_values = data.isnull().sum()
            if missing_values.sum() > 0:
                print("\nValori mancanti rilevati:")
                print(missing_values[missing_values > 0].to_string())
            else:
                print("\nNessun valore mancante rilevato inizialmente.")
            
            return data, data_originale
        except Exception as e:
            print(f"Errore durante l'estrazione: {e}")
            raise