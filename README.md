# Pipeline Dati Dipendenti

Implementazione di una pipeline ETL (Extract, Transform, Load) in Python per elaborare dati sui dipendenti da un file CSV e caricarli in un database SQLite con report e visualizzazioni.
Questo progetto nasce dall'unione di alcuni esercizi svolti finora durante il corso con Generation Italy nel tentativo di unire le competenze che sto sviluppando.

## Installazione

1.  Clonare il repository:
    ```
    git clone https://github.com/LuigiStigliano/Pipeline-Dati-Dipendenti.git
    cd Pipeline-Dati-Dipendenti
    ```

2.  Creare e attivare un ambiente virtuale:
    ```
    # Su Windows
    python -m venv .venv
    .venv\Scripts\activate
    
    # Su macOS/Linux
    python3 -m venv .venv
    source .venv/bin/activate
    ```

3.  Installare le dipendenze:
    ```
    pip install -r requirements.txt
    ```

## Struttura del Progetto

Il codice sorgente è organizzato nella cartella `src/` con i seguenti moduli:
-   `config.py`: Gestisce le configurazioni e i percorsi dei file.
-   `data_extraction.py`: Contiene la logica per l'estrazione dei dati.
-   `data_transformation.py`: Contiene la logica per la trasformazione e pulizia dei dati.
-   `data_loading.py`: Contiene la logica per il caricamento dei dati nel database.
-   `reporting.py`: Contiene la logica per la generazione di report testuali e visualizzazioni.
-   `main_pipeline.py`: Script principale che orchestra l'intera pipeline ETL.

## Utilizzo

1.  Assicurarsi che il file CSV da elaborare sia nella cartella `data/` con il nome `input.csv`.
2.  Eseguire lo script ETL principale:
    ```
    python src/main_pipeline.py
    ```
3.  I risultati verranno salvati nel database SQLite `data/output.db`.
4.  Le visualizzazioni saranno generate nella cartella `data/visualizzazioni/`.

## Funzionalità

### Extract (data_extraction.py)
-   Lettura dei dati dal file CSV.
-   Analisi preliminare con statistiche sui dati di input.
-   Identificazione di valori mancanti e duplicati.

### Transform (data_transformation.py)
-   Pulizia dei dati:
    -   Rimozione duplicati (considerando tutte le colonne tranne l'id; se due record sono uguali in tutte le colonne tranne l'id, solo il primo viene mantenuto).
    -   **Validazione e gestione stipendi negativi**: gli stipendi negativi vengono convertiti in `NaN` e successivamente imputati.
    -   **Validazione e gestione date di assunzione future**: le date di assunzione future rispetto alla data di esecuzione vengono convertite in `NaT` e successivamente imputate.
    -   **Validazione colonne nome, cognome e reparto**: verifica che `nome`, `cognome` e `reparto` siano stringhe valide e non vuote; i record non validi vengono rimossi.
    -   **Validazione e gestione età non valide**: verifica che l'età sia compresa in un intervallo lavorativo ragionevole (es. 16-70 anni) e non sia negativa; i record con età non valide vengono rimossi.
    -   Gestione valori mancanti (per le colonne stipendio e data_assunzione, dopo le validazioni; eventuali valori mancanti in altre colonne non vengono gestiti se non attraverso la rimozione del record per fallimento di altre validazioni).
-   Normalizzazione e conversione dei tipi di dati.
-   Creazione di nuove colonne derivate:
    -   Anni di servizio
    -   Stipendio orario
    -   Fasce di età
    -   Categorizzazione dello stipendio
    -   Calcolo bonus
    -   Classificazione anzianità (Junior, Mid, Senior, Expert)
    -   Valutazione stipendio rispetto alla media del reparto

### Load (data_loading.py)
-   Caricamento dei dati elaborati in un database SQLite.
-   Creazione di viste per analisi specifiche:
    -   Analisi per reparto
    -   Analisi per fascia d'età
    -   Analisi per anzianità

### Report e Visualizzazioni (reporting.py)
-   Generazione di report dettagliati su:
    -   Statistiche di pulizia dati
    -   Analisi per reparto
    -   Analisi per fascia d'età
    -   Analisi per anzianità
    -   Distribuzione delle fasce di stipendio
-   Creazione di visualizzazioni grafiche:
    -   Distribuzione degli stipendi per reparto (boxplot)
    -   Distribuzione delle fasce di stipendio (pie chart)
    -   Numero di dipendenti per reparto (bar chart)
    -   Stipendio medio per fascia d'età (bar chart)
    -   Relazione tra anni di servizio e stipendio (scatter plot)

## Dataset di esempio
Il file CSV di input (`data/input.csv`) (completamente astratto) contiene informazioni sui dipendenti con le seguenti colonne:
-   id
-   nome
-   cognome
-   eta
-   stipendio
-   data_assunzione
-   reparto

## Sviluppi Futuri

Il progetto ha diverse possibilità di evoluzione:

### Miglioramenti Tecnici
-   Aggiungere una suite completa di test
-   Supporto per altri formati di output (Excel, JSON)
-   Supporto per altri database (MySQL, PostgreSQL)

### Nuove Funzionalità
-   Interfaccia web per la visualizzazione dei report
-   Esportazione diretta verso Power BI o Tableau
-   Implementazione di grafici più sofisticati con Seaborn
