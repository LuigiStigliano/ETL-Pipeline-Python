from src import config
from src.data_extraction import DataExtractor
from src.data_transformation import DataTransformer
from src.data_loading import DataLoader
from src.reporting import ReportGenerator

class ETLPipelineOrchestrator:
    def __init__(self, input_path, output_db_path, viz_dir):
        """
        Inizializza l'orchestratore della pipeline ETL.
        Args:
            input_path (str): Percorso del file CSV di input.
            output_db_path (str): Percorso del database SQLite di output.
            viz_dir (str): Directory per salvare le visualizzazioni.
        """
        self.extractor = DataExtractor(input_path)
        self.transformer = DataTransformer()
        self.loader = DataLoader(output_db_path)
        self.reporter = ReportGenerator(viz_dir)
        
        self.raw_data = None
        self.original_data_copy = None # Per statistiche o confronti futuri se necessario
        self.transformed_data = None
        self.transform_stats = None

    def run_etl(self):
        """Esegue i passaggi Extract, Transform, Load."""
        try:
            self.raw_data, self.original_data_copy = self.extractor.extract_data()
            if self.raw_data is not None:
                self.transformed_data, self.transform_stats = self.transformer.transform_data(self.raw_data)
                if self.transformed_data is not None:
                    self.loader.load_data(self.transformed_data)
                else:
                    print("Trasformazione non ha prodotto dati, caricamento saltato.")
            else:
                print("Estrazione non ha prodotto dati, pipeline interrotta.")
        except Exception as e:
            print(f"Errore durante l'esecuzione ETL: {e}")
            # Potrebbe essere utile propagare l'eccezione o gestirla più specificamente
            raise 

    def run_reporting(self):
        """Genera report e visualizzazioni."""
        if self.transformed_data is None or self.transform_stats is None:
            print("Dati trasformati non disponibili. Impossibile generare report e visualizzazioni.")
            print("Assicurarsi di aver eseguito run_etl() con successo.")
            return

        try:
            self.reporter.generate_text_report(self.transformed_data, self.transform_stats)
            self.reporter.generate_visualizations(self.transformed_data)
        except Exception as e:
            print(f"Errore durante la generazione di report/visualizzazioni: {e}")
            # Potrebbe essere utile propagare l'eccezione

    def run_full_pipeline(self):
        """Esegue l'intera pipeline ETL con report e visualizzazioni."""
        print("Avvio della pipeline ETL completa...")
        self.run_etl()
        self.run_reporting()
        print("\nPipeline ETL completata.")
        print(f"I dati elaborati sono stati salvati in: {config.OUTPUT_DB_PATH}")
        print(f"Le visualizzazioni sono state salvate in: {config.VISUALIZATIONS_DIR}")

def main():
    """
    Funzione principale che inizializza e avvia la pipeline ETL.
    """
    pipeline = ETLPipelineOrchestrator(
        input_path=config.INPUT_CSV_PATH,
        output_db_path=config.OUTPUT_DB_PATH,
        viz_dir=config.VISUALIZATIONS_DIR
    )
    try:
        pipeline.run_full_pipeline()
        print("\nEsecuzione della pipeline terminata con successo!")
    except Exception as e:
        print(f"\nERRORE CRITICO durante l'esecuzione della pipeline: {e}")

if __name__ == "__main__":
    main()
