# ETL Pipeline Python

Implementazione di una pipeline ETL (Extract, Transform, Load) in Python per elaborare dati da un file CSV e caricarli in un database SQLite con report e visualizzazioni.
Questo progetto nasce dall'unione di alcuni esercizi svolti finora durante il corso con Generation Italy nel tentativo di unire le competenze che sto sviluppando.

## Installazione

1. Clonare il repository:
   ```
   git clone https://github.com/LuigiStigliano/ETL-Pipeline-Python.git
   cd ETL-Pipeline-Python
   ```

2. Creare e attivare un ambiente virtuale:
   ```
   # Su Windows
   python -m venv .venv
   .venv\Scripts\activate
   
   # Su macOS/Linux
   python3 -m venv .venv
   source .venv/bin/activate
   ```

3. Installare le dipendenze:
   ```
   pip install -r requirements.txt
   ```

## Utilizzo

1. Mettere il file CSV da elaborare nella cartella `data/` con il nome `input.csv`
2. Eseguire lo script ETL:
   ```
   python src/etl_pipeline.py
   ```
3. I risultati verranno salvati nel database SQLite `data/output.db`
4. Le visual saranno generate nella cartella `data/visualizzazioni/`

## Funzionalità

### Extract
- Lettura dei dati dal file CSV
- Analisi preliminare con statistiche sui dati di input
- Identificazione di valori mancanti e duplicati

### Transform
- Pulizia dei dati
  - rimozione duplicati (considerando tutte le colonne tranne l'id; se due record sono uguali in tutte le colonne tranne l'id, solo il primo viene mantenuto)
  - gestione valori mancanti (solo per le colonne stipendio e data_assunzione; eventuali valori mancanti in altre colonne non vengono gestiti)
- Normalizzazione e conversione dei tipi di dati
- Creazione di nuove colonne derivate:
  - Anni di servizio
  - Stipendio orario
  - Fasce di età
  - Categorizzazione dello stipendio
  - Calcolo bonus
  - Classificazione anzianità (Junior, Mid, Senior, Expert)
  - Valutazione stipendio rispetto alla media del reparto

### Load
- Caricamento dei dati elaborati in un database SQLite
- Creazione di viste per analisi specifiche:
  - Analisi per reparto
  - Analisi per fascia d'età
  - Analisi per anzianità

### Report e Visualizzazioni
- Generazione di report dettagliati su:
  - Statistiche di pulizia dati
  - Analisi per reparto
  - Analisi per fascia d'età
  - Analisi per anzianità
  - Distribuzione delle fasce di stipendio
- Creazione di visualizzazioni grafiche:
  - Distribuzione degli stipendi per reparto (boxplot)
  - Distribuzione delle fasce di stipendio (pie chart)
  - Numero di dipendenti per reparto (bar chart)
  - Stipendio medio per fascia d'età (bar chart)
  - Relazione tra anni di servizio e stipendio (scatter plot)

## Dataset di esempio
Il file CSV di input (completamente astratto) contiene informazioni sui dipendenti con le seguenti colonne:
- id
- nome
- cognome
- eta
- stipendio
- data_assunzione
- reparto

## Contributing

Siamo felici di accettare contributi al progetto! Ecco come puoi contribuire:

1. **Forka il repository** e crea un branch per le tue modifiche
2. **Fai le tue modifiche** seguendo le linee guida del codice:
   - Usa Python 3.9 o superiore
   - Segui le convenzioni PEP8
   - Usa docstring per tutte le funzioni pubbliche
   - Blocca le versioni delle dipendenze in `requirements.txt`
3. **Esegui i test** per assicurarti che tutto funzioni correttamente
4. **Crea una Pull Request** spiegando le tue modifiche

Per segnalare bug o richiedere nuove funzionalità, apri una [Issue](https://github.com/LuigiStigliano/ETL-Pipeline-Python/issues).

## Sviluppi Futuri

Il progetto ha diverse possibilità di evoluzione:

### Miglioramenti nella Validazione dei Dati
- Controlli per stipendi negativi e date di assunzione future
- Validazione delle colonne nome, cognome, età e reparto
- Gestione di età non valide (superiori a quella lavorativa o negative)

### Miglioramenti Tecnici
- Rendere il codice più modulare
- Aggiungere una suite completa di test
- Supporto per altri formati di output (Excel, JSON)
- Supporto per altri database (MySQL, PostgreSQL)

### Nuove Funzionalità
- Interfaccia web per la visualizzazione dei report
- Esportazione diretta verso Power BI o Tableau
- Implementazione di grafici più sofisticati con Seaborn

Per domande o suggerimenti, non esitare ad aprire una Issue nel repository.
