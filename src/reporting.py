import os
import matplotlib.pyplot as plt
import pandas as pd
from src import config

class ReportGenerator:
    def __init__(self, visualizations_dir):
        """
        Inizializza il generatore di report e visualizzazioni.
        Args:
            visualizations_dir (str): Directory dove salvare le visualizzazioni.
        """
        self.visualizations_dir = visualizations_dir
        os.makedirs(self.visualizations_dir, exist_ok=True)

    def generate_text_report(self, df, transform_stats):
        """
        Genera un report testuale sui dati elaborati.
        Args:
            df (pd.DataFrame): DataFrame elaborato.
            transform_stats (dict): Statistiche dalla fase di trasformazione.
        Returns:
            dict: Un dizionario contenente le statistiche del report.
        """
        if df is None:
            raise ValueError("Nessun dato disponibile per il report.")
        
        print("\n=== Report sui Dati Elaborati ===")
        print(f"Numero totale di dipendenti dopo trasformazione: {len(df)}")
        
        print("\n1. Statistiche di Pulizia e Imputazione Dati:")
        print(f"- Valori mancanti per stipendio (imputati): {transform_stats.get('missing_stipendio_imputed', 'N/A')}")
        print(f"- Valori mancanti per data assunzione (imputati): {transform_stats.get('missing_data_assunzione_imputed', 'N/A')}")
        print(f"- Duplicati rimossi: {transform_stats.get('duplicati_rimossi', 'N/A')}")
        
        print("\n2. Statistiche per Reparto (dati trasformati):")
        reparto_stats = df.groupby(config.DEPARTMENT_COLUMN).agg(
            numero_dipendenti=('id', 'count'),
            stipendio_medio=(config.SALARY_COLUMN, 'mean'),
            stipendio_min=(config.SALARY_COLUMN, 'min'),
            stipendio_max=(config.SALARY_COLUMN, 'max'),
            eta_media=(config.AGE_COLUMN, 'mean'),
            media_anni_servizio=('anni_di_servizio', 'mean'),
            totale_bonus=('bonus', 'sum')
        ).round(2)
        print(reparto_stats)
        
        print("\n3. Statistiche per Fascia d'Età (dati trasformati):")
        eta_stats = df.groupby('fascia_eta', observed=False).agg(
            numero_dipendenti=('id', 'count'),
            stipendio_medio=(config.SALARY_COLUMN, 'mean'),
            stipendio_min=(config.SALARY_COLUMN, 'min'),
            stipendio_max=(config.SALARY_COLUMN, 'max'),
            media_anni_servizio=('anni_di_servizio', 'mean')
        ).round(2)
        print(eta_stats)
        
        print("\n4. Statistiche per Anzianità (dati trasformati):")
        anzianita_stats = df.groupby('anzianita', observed=False).agg(
            numero_dipendenti=('id', 'count'),
            stipendio_medio=(config.SALARY_COLUMN, 'mean'),
            stipendio_min=(config.SALARY_COLUMN, 'min'),
            stipendio_max=(config.SALARY_COLUMN, 'max')
        ).round(2)
        print(anzianita_stats)
        
        print("\n5. Distribuzione Fasce di Stipendio (dati trasformati):")
        stipendio_distribution = df['fascia_stipendio'].value_counts().sort_index()
        print(stipendio_distribution.to_string())
        
        print("\n6. Distribuzione Valutazione Stipendio (dati trasformati):")
        valutazione_distribution = df['valutazione_stipendio'].value_counts()
        print(valutazione_distribution.to_string())
        
        return {
            'reparto_stats': reparto_stats,
            'eta_stats': eta_stats,
            'anzianita_stats': anzianita_stats,
            'stipendio_distribution': stipendio_distribution,
            'valutazione_distribution': valutazione_distribution
        }

    def generate_visualizations(self, df):
        """
        Genera visualizzazioni grafiche dai dati elaborati.
        Args:
            df (pd.DataFrame): DataFrame elaborato.
        """
        if df is None:
            raise ValueError("Nessun dato disponibile per le visualizzazioni.")
        
        print(f"\nGenerazione delle visualizzazioni in {self.visualizations_dir}...")

        # 1. Distribuzione degli stipendi per reparto (boxplot)
        plt.figure(figsize=(12, 7))
        df.boxplot(column=config.SALARY_COLUMN, by=config.DEPARTMENT_COLUMN, grid=True)
        plt.title('Distribuzione degli Stipendi per Reparto')
        plt.suptitle('') 
        plt.xlabel('Reparto')
        plt.ylabel('Stipendio (€)')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(self.visualizations_dir, 'stipendi_per_reparto.png'))
        plt.close()
        
        # 2. Distribuzione delle fasce di stipendio (pie chart)
        plt.figure(figsize=(8, 8))
        df['fascia_stipendio'].value_counts().plot.pie(autopct='%1.1f%%', startangle=90, wedgeprops={'edgecolor': 'black'})
        plt.title('Distribuzione delle Fasce di Stipendio')
        plt.ylabel('') 
        plt.tight_layout()
        plt.savefig(os.path.join(self.visualizations_dir, 'fasce_stipendio.png'))
        plt.close()
        
        # 3. Numero di dipendenti per reparto (bar chart)
        plt.figure(figsize=(10, 7))
        df[config.DEPARTMENT_COLUMN].value_counts().sort_values().plot.barh(color='skyblue', edgecolor='black')
        plt.title('Numero di Dipendenti per Reparto')
        plt.xlabel('Numero di Dipendenti')
        plt.ylabel('Reparto')
        plt.tight_layout()
        plt.savefig(os.path.join(self.visualizations_dir, 'dipendenti_per_reparto.png'))
        plt.close()
        
        # 4. Stipendio medio per fascia d'età (bar chart)
        plt.figure(figsize=(10, 7))
        df.groupby('fascia_eta', observed=False)[config.SALARY_COLUMN].mean().plot.bar(color='lightcoral', edgecolor='black')
        plt.title('Stipendio Medio per Fascia d\'Età')
        plt.ylabel('Stipendio Medio (€)')
        plt.xlabel('Fascia d\'Età')
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig(os.path.join(self.visualizations_dir, 'stipendio_medio_per_eta.png'))
        plt.close()
        
        # 5. Relazione tra anni di servizio e stipendio (scatter plot)
        plt.figure(figsize=(10, 7))
        scatter = plt.scatter(df['anni_di_servizio'], df[config.SALARY_COLUMN], 
                              alpha=0.6, c=df[config.DEPARTMENT_COLUMN].astype('category').cat.codes, cmap='viridis')
        plt.title('Relazione tra Anni di Servizio e Stipendio')
        plt.xlabel('Anni di Servizio')
        plt.ylabel('Stipendio (€)')
        
        # Creazione legenda per i reparti
        handles, _ = scatter.legend_elements(prop="colors", alpha=0.6)
        department_categories = df[config.DEPARTMENT_COLUMN].astype('category').cat.categories
        if len(handles) == len(department_categories): # Aggiungi legenda solo se i colori corrispondono
             plt.legend(handles, department_categories, title="Reparto")
        else:
            print("Attenzione: non è stato possibile generare una legenda accurata per lo scatter plot dei reparti.")

        plt.grid(True)
        plt.tight_layout()
        plt.savefig(os.path.join(self.visualizations_dir, 'stipendio_vs_anzianita.png'))
        plt.close()
        
        print(f"Visualizzazioni salvate con successo.")