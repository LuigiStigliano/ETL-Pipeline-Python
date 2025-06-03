# Pipeline Dati Dipendenti

Implementazione di una pipeline ETL (Extract, Transform, Load) in Python per elaborare dati sui dipendenti da un file CSV, caricarli in un database SQLite e visualizzare i risultati tramite un'interfaccia web interattiva costruita con Streamlit.
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

3.  Installare le dipendenze (incluso Streamlit, ora presente in `requirements.txt`):
    ```
    pip install -r requirements.txt
    ```

## Struttura del Progetto

Il codice sorgente è organizzato nella cartella `src/` con i seguenti moduli principali, più l'applicazione Streamlit nella root:
-   `streamlit_app.py`: Applicazione web interattiva basata su Streamlit per visualizzare i report e avviare la pipeline. (Situata nella directory principale del progetto)
-   `src/config.py`: Gestisce le configurazioni e i percorsi dei file.
-   `src/data_extraction.py`: Contiene la logica per l'estrazione dei dati.
-   `src/data_transformation.py`: Contiene la logica per la trasformazione e pulizia dei dati.
-   `src/data_loading.py`: Contiene la logica per il caricamento dei dati nel database.
-   `src/reporting.py`: Contiene la logica per la generazione di report testuali e visualizzazioni (utilizzata dalla pipeline principale e come riferimento per Streamlit).
-   `src/main_pipeline.py`: Script principale che orchestra l'intera pipeline ETL (può essere eseguito anche dall'interfaccia Streamlit).

## Utilizzo

1.  Assicurarsi che il file CSV da elaborare sia nella cartella `data/` con il nome `input.csv` (se si desidera eseguire la pipeline per la prima volta o con nuovi dati).
2.  **Per avviare l'interfaccia web interattiva (consigliato):**
    Eseguire dalla directory principale del progetto:
    ```
    streamlit run streamlit_app.py
    ```
    Questo aprirà l'applicazione nel tuo browser, da cui potrai visualizzare i report (se il database `output.db` esiste) e/o avviare l'esecuzione della pipeline ETL.

3.  **Per eseguire solo la pipeline ETL da riga di comando (alternativa):**
    ```
    python src/main_pipeline.py
    ```
4.  I risultati della pipeline ETL verranno salvati nel database SQLite `data/output.db`.
5.  Le visualizzazioni basate su file (generate dalla pipeline da riga di comando) saranno create nella cartella `data/visualizzazioni/`. L'interfaccia Streamlit genera le visualizzazioni dinamicamente.

## Funzionalità

### Extract (data_extraction.py)
-   Lettura dei dati dal file CSV.
-   Analisi preliminare con statistiche sui dati di input.
-   Identificazione di valori mancanti.

### Transform (data_transformation.py)
-   Pulizia dei dati:
    -   Rimozione duplicati.
    -   Validazione e gestione stipendi negativi.
    -   Validazione e gestione date di assunzione future.
    -   Validazione colonne nome, cognome e reparto.
    -   Validazione e gestione età non valide.
    -   Gestione valori mancanti per stipendio e data_assunzione.
-   Normalizzazione e conversione dei tipi di dati.
-   Creazione di nuove colonne derivate (Anni di servizio, Stipendio orario, Fasce di età, ecc.).

### Load (data_loading.py)
-   Caricamento dei dati elaborati in un database SQLite.
-   Creazione di viste SQL per analisi specifiche.

### Report e Visualizzazioni (reporting.py e Interfaccia Streamlit)
-   **`reporting.py` (per la pipeline da riga di comando):**
    -   Generazione di report testuali dettagliati.
    -   Creazione e salvataggio di visualizzazioni grafiche su file (boxplot, pie chart, bar chart, scatter plot).
-   **Interfaccia Web con Streamlit (`streamlit_app.py`):**
    -   Visualizzazione interattiva dei dati aggregati e delle statistiche direttamente dal database.
    -   Generazione dinamica di grafici (boxplot, bar chart, pie chart, scatter plot) per l'esplorazione dei dati.
    -   Possibilità di avviare l'intera pipeline ETL direttamente dall'interfaccia web.
    -   Report suddivisi per sezioni navigabili (Panoramica, Analisi per Reparto, Età, Anzianità, Distribuzione Stipendi).

## Dataset di esempio
Il file CSV di input (`data/input.csv`) (completamente astratto) contiene informazioni sui dipendenti con le seguenti colonne:
-   id
-   nome
-   cognome
-   eta
-   stipendio
-   data_assunzione
-   reparto
