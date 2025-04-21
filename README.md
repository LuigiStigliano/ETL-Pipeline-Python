# ETL Pipeline Python

Implementazione di una pipeline ETL (Extract, Transform, Load) in Python per elaborare dati da un file CSV e caricarli in un database SQLite con report e visualizzazioni.
Questo progetto nasce dall'unione di alcuni esercizi svolti finora durante il corso con Generation Italy nel tentativo di unire le competenze che sto sviluppando.

## Struttura del Progetto

```
ETL-Pipeline-Python/
├── data/
│   ├── input.csv         # Dati grezzi da elaborare
│   ├── output.db         # Database SQLite con i dati elaborati
│   └── visualizzazioni/  # Directory generata con grafici e visualizzazioni
├── src/
│   └── etl_pipeline.py   # Script principale per l'ETL
├── requirements.txt      # Elenco delle dipendenze
├── LICENSE               # Informazioni sulla licenza del progetto
├── CONTRIBUTING.md       # Linee guida per chi desidera contribuire al progetto
└── README.md             # Documentazione e istruzioni dell'uso
```

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
