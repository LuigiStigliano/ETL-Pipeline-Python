# CONTRIBUTING.md

Grazie per il tuo interesse nel contribuire a questo progetto ETL Pipeline Python!

## Come contribuire

1. **Forka il repository**  
   Clicca su "Fork" in alto a destra per creare una copia del repository sul tuo account GitHub.

2. **Crea un branch**  
   Crea un branch descrittivo per la tua modifica:
   ```
   git checkout -b nome-feature
   ```

3. **Fai le tue modifiche**  
   - Segui lo stile del codice esistente.
   - Aggiungi o aggiorna i test se necessario.
   - Aggiorna la documentazione se cambi l’interfaccia o il comportamento.

4. **Esegui i test**  
   Assicurati che tutti i test passino prima di inviare la tua pull request:
   ```
   pytest
   ```

5. **Crea una Pull Request**  
   - Spiega cosa hai cambiato e perché.
   - Collega eventuali issue correlate.

## Linee guida per il codice

- Usa Python 3.9 o superiore.
- Segui le convenzioni PEP8.
- Usa docstring per tutte le funzioni pubbliche.
- Blocca le versioni delle dipendenze in `requirements.txt`.

## Idee per estensioni future

Ecco alcune possibili evoluzioni del progetto:
- Aggiungere controlli per stipendi negativi e date di assunzione future e scegliere come trasformarli
- Aggiungere controlli per le colonne nome, cognome, età e reparto
- Aggiungere controlli per età superiori a quella lavorativa o età negativa e scegliere come trasformarli
- Rendere modulare il codice
- Aggiungere la sezione dei tests
- Aggiungere funzionalità di esportazione in altri formati (Excel, JSON)
- Implementare un'interfaccia web per visualizzare i report
- Esportazione diretta verso strumenti come Power BI o Tableau
- Consideerare l'utilizzo si Seaborn per grafici più sofisticati
- Aggiungere supporto per altri database (MySQL, PostgreSQL)

## Segnalazione bug e richieste di funzionalità

- Per segnalare un bug, apri una [Issue](https://github.com/LuigiStigliano/ETL-Pipeline-Python/issues) e fornisci dettagli e passi per riprodurlo.
- Per proporre nuove funzionalità, apri una Issue descrivendo la motivazione e l’implementazione desiderata.

## Domande

Per qualsiasi domanda apri una Issue.
Grazie per il tuo contributo!
