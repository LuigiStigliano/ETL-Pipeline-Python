import sqlite3
import os
import pandas as pd
from src import config

class DataLoader:
    def __init__(self, db_path):
        """
        Inizializza il caricatore di dati.
        Args:
            db_path (str): Percorso del database SQLite di output.
        """
        self.db_path = db_path

    def load_data(self, df):
        """
        Carica i dati trasformati in un database SQLite e crea viste.
        Args:
            df (pd.DataFrame): DataFrame trasformato da caricare.
        Raises:
            ValueError: Se il DataFrame è None.
            Exception: Se si verifica un errore durante il caricamento.
        """
        print(f"\nCaricamento dei dati in {self.db_path}...")
        if df is None:
            raise ValueError("Nessun dato da caricare. Esegui prima la trasformazione.")

        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        conn = sqlite3.connect(self.db_path)

        try:
            df.to_sql('dipendenti', conn, if_exists='replace', index=False)
            
            conn.execute('''
                CREATE VIEW IF NOT EXISTS analisi_per_reparto AS
                SELECT 
                    reparto,
                    COUNT(*) as numero_dipendenti,
                    AVG(stipendio) as stipendio_medio,
                    MIN(stipendio) as stipendio_min,
                    MAX(stipendio) as stipendio_max,
                    AVG(anni_di_servizio) as media_anni_servizio,
                    SUM(bonus) as totale_bonus
                FROM dipendenti
                GROUP BY reparto
            ''')
            
            conn.execute('''
                CREATE VIEW IF NOT EXISTS analisi_per_fascia_eta AS
                SELECT 
                    fascia_eta,
                    COUNT(*) as numero_dipendenti,
                    AVG(stipendio) as stipendio_medio,
                    MIN(stipendio) as stipendio_min,
                    MAX(stipendio) as stipendio_max,
                    AVG(anni_di_servizio) as media_anni_servizio
                FROM dipendenti
                GROUP BY fascia_eta
            ''')
            
            conn.execute('''
                CREATE VIEW IF NOT EXISTS analisi_per_anzianita AS
                SELECT 
                    anzianita,
                    COUNT(*) as numero_dipendenti,
                    AVG(stipendio) as stipendio_medio,
                    MIN(stipendio) as stipendio_min,
                    MAX(stipendio) as stipendio_max
                FROM dipendenti
                GROUP BY anzianita
            ''')
            
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM dipendenti')
            total_records = cursor.fetchone()[0]
            
            print(f"Caricati con successo {total_records} record nel database.")
            print("Viste create/aggiornate: analisi_per_reparto, analisi_per_fascia_eta, analisi_per_anzianita")
            
        except Exception as e:
            print(f"Errore durante il caricamento: {e}")
            raise
        finally:
            conn.close()