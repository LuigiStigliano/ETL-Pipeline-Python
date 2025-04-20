import os
import sqlite3
import pandas as pd
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt

class ETLPipeline:
    def __init__(self, input_path, output_db):
        """
        Inizializza la pipeline ETL.
        """
        self.input_path = input_path
        self.output_db = output_db
        self.data = None
        self.data_originale = None
        self.output_dir = os.path.dirname(output_db)
        
    def extract(self):
        """
        Estrae i dati dal file CSV.
        """
        print(f"Estraendo dati da {self.input_path}...")
        try:
            self.data = pd.read_csv(self.input_path)
            # Salviamo una copia dei dati originali per confronti
            self.data_originale = self.data.copy()
            print(f"Estratti {len(self.data)} record.")
            
            # Mostriamo alcune statistiche iniziali
            print("\nStatistiche iniziali:")
            print(f"Numero di record: {len(self.data)}")
            print(f"Numero di colonne: {len(self.data.columns)}")
            print(f"Reparti presenti: {self.data['reparto'].unique()}")
            print(f"Range età: {self.data['eta'].min()} - {self.data['eta'].max()}")
            print(f"Range stipendio: {self.data['stipendio'].min()} - {self.data['stipendio'].max()}")
            
            # Verifica dei valori mancanti
            missing_values = self.data.isnull().sum()
            if missing_values.sum() > 0:
                print("\nValori mancanti rilevati:")
                print(missing_values[missing_values > 0].to_string())
            
        except Exception as e:
            print(f"Errore durante l'estrazione: {e}")
            raise
        
        return self
    
    def transform(self):
        """
        Trasforma e pulisce i dati.
        """
        print("\nTrasformando i dati...")
        if self.data is None:
            raise ValueError("Nessun dato da trasformare. Esegui prima extract().")
        
        # Copia per evitare warning di SettingWithCopyWarning
        df = self.data.copy()
        
        # 1. Rimuovere i duplicati
        df_size = len(df)
        df.drop_duplicates(subset=[col for col in df.columns if col != 'id'], inplace=True)
        duplicati_rimossi = df_size - len(df)
        print(f"Rimossi {duplicati_rimossi} record duplicati.")
        
        # 2. Gestire i valori mancanti
        # Salviamo le informazioni sui dati mancanti per il report
        missing_stipendio_count = df['stipendio'].isna().sum()
        missing_data_assunzione_count = df['data_assunzione'].isna().sum()
        
        # Calcolo dello stipendio medio per reparto e per fascia d'età
        stipendio_medio_reparto = df.groupby('reparto')['stipendio'].mean().to_dict()
        
        # Categorizzazione delle fasce d'età per analisi
        df['fascia_eta'] = pd.cut(
            df['eta'],
            bins=[0, 30, 40, 50, float('inf')],
            labels=['20-30', '31-40', '41-50', '50+']
        )
        
        # Sostituiamo gli stipendi mancanti con la media del reparto
        for i, row in df[df['stipendio'].isna()].iterrows():
            reparto = row['reparto']
            df.at[i, 'stipendio'] = stipendio_medio_reparto.get(reparto, df['stipendio'].mean())
        
        # Sostituiamo le date di assunzione mancanti con la mediana delle date per reparto
        df['data_assunzione'] = pd.to_datetime(df['data_assunzione'], errors='coerce')
        date_mediane_reparto = df.groupby('reparto')['data_assunzione'].median().to_dict()
        
        for i, row in df[df['data_assunzione'].isna()].iterrows():
            reparto = row['reparto']
            df.at[i, 'data_assunzione'] = date_mediane_reparto.get(reparto, df['data_assunzione'].median())
        
        # 3. Convertire i tipi di dati
        df['eta'] = df['eta'].astype(int)
        df['stipendio'] = df['stipendio'].astype(float)
        
        # 4. Creare nuove colonne derivate
        # Calcoliamo anni di servizio e stipendio annualizzato
        df['data_assunzione'] = pd.to_datetime(df['data_assunzione'])
        df['anni_di_servizio'] = (datetime.now() - df['data_assunzione']).dt.days / 365.25
        df['anni_di_servizio'] = df['anni_di_servizio'].round(1)
        
        # Calcoliamo lo stipendio orario (assumendo 40 ore settimanali per 52 settimane)
        df['stipendio_orario'] = round(df['stipendio'] / (40 * 52), 2)
        
        # 5. Categorizzare lo stipendio
        df['fascia_stipendio'] = pd.cut(
            df['stipendio'],
            bins=[0, 40000, 50000, float('inf')],
            labels=['Basso', 'Medio', 'Alto']
        )
        
        # 6. Aggiungiamo una colonna per il bonus basato sugli anni di servizio
        df['bonus'] = df['anni_di_servizio'].apply(
            lambda anni: 500 if anni < 2 else (1000 if anni < 5 else 2000)
        )
        
        # 7. Aggiungiamo una colonna per anzianità
        df['anzianita'] = pd.cut(
            df['anni_di_servizio'],
            bins=[-1, 2, 5, 8, float('inf')],
            labels=['Junior', 'Mid', 'Senior', 'Expert']
        )
        
        # 8. Aggiungiamo una colonna per la valutazione del livello stipendio rispetto alla media del reparto
        media_stipendi_per_reparto = df.groupby('reparto')['stipendio'].transform('mean')
        df['valutazione_stipendio'] = np.where(
            df['stipendio'] > media_stipendi_per_reparto * 1.1, 'Sopra Media',
            np.where(df['stipendio'] < media_stipendi_per_reparto * 0.9, 'Sotto Media', 'Nella Media')
        )
        
        # Salviamo le statistiche dei dati trasformati
        self.stats = {
            'missing_stipendio': missing_stipendio_count,
            'missing_data_assunzione': missing_data_assunzione_count,
            'duplicati_rimossi': duplicati_rimossi,
            'stipendio_medio_per_reparto': stipendio_medio_reparto,
            'conteggio_per_reparto': df['reparto'].value_counts().to_dict()
        }
        
        self.data = df
        print("Trasformazione completata.")
        return self
    
    def load(self):
        """
        Carica i dati trasformati in un database SQLite.
        """
        print(f"\nCaricamento dei dati in {self.output_db}...")
        if self.data is None:
            raise ValueError("Nessun dato da caricare. Esegui prima transform().")
        
        # Assicurarsi che la directory esista
        os.makedirs(os.path.dirname(self.output_db), exist_ok=True)
        
        # Connessione al database
        conn = sqlite3.connect(self.output_db)
        
        try:
            # Creare la tabella dipendenti
            self.data.to_sql('dipendenti', conn, if_exists='replace', index=False)
            
            # Creare una vista per l'analisi per reparto
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
            
            # Creare una vista per l'analisi per fascia d'età
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
            
            # Creare una vista per l'analisi per anzianità
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
            
            # Eseguiamo una query per ottenere statistiche riassuntive
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) FROM dipendenti')
            total_records = cursor.fetchone()[0]
            
            print(f"Caricati con successo {total_records} record nel database.")
            print("Viste create per l'analisi: analisi_per_reparto, analisi_per_fascia_eta, analisi_per_anzianita")
            
        except Exception as e:
            print(f"Errore durante il caricamento: {e}")
            raise
        finally:
            conn.close()
        
        return self
    
    def generate_report(self):
        """
        Genera un report sui dati elaborati.
        """
        if self.data is None:
            raise ValueError("Nessun dato disponibile. Esegui prima la pipeline.")
        
        print("\n=== Report sui Dati Elaborati ===")
        print(f"Numero totale di dipendenti: {len(self.data)}")
        
        # Statistiche sui dati in entrata
        print("\n1. Statistiche di Pulizia Dati:")
        print(f"- Valori mancanti per stipendio: {self.stats['missing_stipendio']}")
        print(f"- Valori mancanti per data assunzione: {self.stats['missing_data_assunzione']}")
        print(f"- Duplicati rimossi: {self.stats['duplicati_rimossi']}")
        
        # Statistiche per reparto
        print("\n2. Statistiche per Reparto:")
        reparto_stats = self.data.groupby('reparto').agg({
            'id': 'count',
            'stipendio': ['mean', 'min', 'max'],
            'eta': 'mean',
            'anni_di_servizio': 'mean',
            'bonus': 'sum'
        })
        print(reparto_stats)
        
        # Statistiche per fascia d'età
        print("\n3. Statistiche per Fascia d'Età:")
        eta_stats = self.data.groupby('fascia_eta', observed=False).agg({
            'id': 'count',
            'stipendio': ['mean', 'min', 'max'],
            'anni_di_servizio': 'mean'
        })
        print(eta_stats)
        
        # Statistiche per anzianità
        print("\n4. Statistiche per Anzianità:")
        anzianita_stats = self.data.groupby('anzianita', observed=False).agg({
            'id': 'count',
            'stipendio': ['mean', 'min', 'max']
        })
        print(anzianita_stats)
        
        # Distribuzione fasce di stipendio
        print("\n5. Distribuzione Fasce di Stipendio:")
        stipendio_distribution = self.data['fascia_stipendio'].value_counts().sort_index()
        print(stipendio_distribution.to_string())
        
        # Distribuzione valutazione stipendio
        print("\n6. Distribuzione Valutazione Stipendio:")
        valutazione_distribution = self.data['valutazione_stipendio'].value_counts()
        print(valutazione_distribution.to_string())
        
        return {
            'reparto_stats': reparto_stats,
            'eta_stats': eta_stats,
            'anzianita_stats': anzianita_stats,
            'stipendio_distribution': stipendio_distribution,
            'valutazione_distribution': valutazione_distribution
        }
    
    def generate_visualizations(self):
        """
        Genera visualizzazioni dai dati elaborati.
        """
        if self.data is None:
            raise ValueError("Nessun dato disponibile. Esegui prima la pipeline.")
        
        print("\nGenerazione delle visual...")
        
        # Creare directory per le visualizzazioni se non esiste
        viz_dir = os.path.join(self.output_dir, 'visualizzazioni')
        os.makedirs(viz_dir, exist_ok=True)
        
        # 1. Distribuzione degli stipendi per reparto (boxplot)
        plt.figure(figsize=(12, 6))
        self.data.boxplot(column='stipendio', by='reparto')
        plt.title('Distribuzione degli Stipendi per Reparto')
        plt.suptitle('')  # Rimuovi il titolo automatico
        plt.ylabel('Stipendio (€)')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'stipendi_per_reparto.png'))
        plt.close()
        
        # 2. Distribuzione delle fasce di stipendio (pie chart)
        plt.figure(figsize=(8, 8))
        self.data['fascia_stipendio'].value_counts().plot.pie(autopct='%1.1f%%')
        plt.title('Distribuzione delle Fasce di Stipendio')
        plt.ylabel('')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'fasce_stipendio.png'))
        plt.close()
        
        # 3. Numero di dipendenti per reparto (bar chart)
        plt.figure(figsize=(10, 6))
        self.data['reparto'].value_counts().sort_values().plot.barh()
        plt.title('Numero di Dipendenti per Reparto')
        plt.xlabel('Numero di Dipendenti')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'dipendenti_per_reparto.png'))
        plt.close()
        
        # 4. Stipendio medio per fascia d'età (bar chart)
        plt.figure(figsize=(10, 6))
        self.data.groupby('fascia_eta', observed=False)['stipendio'].mean().plot.bar()
        plt.title('Stipendio Medio per Fascia d\'Età')
        plt.ylabel('Stipendio Medio (€)')
        plt.xlabel('Fascia d\'Età')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'stipendio_medio_per_eta.png'))
        plt.close()
        
        # 5. Relazione tra anzianità e stipendio (scatter plot)
        plt.figure(figsize=(10, 6))
        plt.scatter(self.data['anni_di_servizio'], self.data['stipendio'], 
                    alpha=0.6, c=self.data['reparto'].astype('category').cat.codes)
        plt.title('Relazione tra Anni di Servizio e Stipendio')
        plt.xlabel('Anni di Servizio')
        plt.ylabel('Stipendio (€)')
        plt.colorbar(ticks=range(len(self.data['reparto'].unique())), 
                    label='Reparto')
        plt.tight_layout()
        plt.savefig(os.path.join(viz_dir, 'stipendio_vs_anzianita.png'))
        plt.close()
        
        print(f"Visual salvate in {viz_dir}")
        return viz_dir
    
    def run(self):
        """
        Esegue l'intera pipeline ETL.
        """
        return self.extract().transform().load()
    
    def run_full_pipeline(self):
        """
        Esegue l'intera pipeline ETL con report e visualizzazioni.
        """
        self.run()
        self.generate_report()
        self.generate_visualizations()
        return self

def main():
    """
    Funzione principale che esegue la pipeline ETL.
    """
    # Percorsi dei file
    current_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    input_path = os.path.join(current_dir, 'data', 'input.csv')
    output_db = os.path.join(current_dir, 'data', 'output.db')
    
    # Creare e avviare la pipeline
    pipeline = ETLPipeline(input_path, output_db)
    
    try:
        print("Avvio della pipeline ETL...")
        pipeline.run_full_pipeline()
        print("\nPipeline ETL completata con successo!")
        print(f"I dati elaborati sono stati salvati in: {output_db}")
        print(f"Le visual sono state salvate in: {os.path.join(os.path.dirname(output_db), 'visualizzazioni')}")
    except Exception as e:
        print(f"\nErrore durante l'esecuzione della pipeline: {e}")

if __name__ == "__main__":
    main()