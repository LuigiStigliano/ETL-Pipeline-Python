import streamlit as st
import pandas as pd
import sqlite3
import matplotlib.pyplot as plt
import os

from src import config
from src.main_pipeline import ETLPipelineOrchestrator 

# --- Configurazione Globale ---
DB_PATH = config.OUTPUT_DB_PATH # Percorso al database SQLite
# Verifica se il database esiste, altrimenti suggerisci di eseguire l'ETL
db_exists = os.path.exists(DB_PATH)

# --- Funzioni Helper ---
@st.cache_data # Ottimo per memorizzare nella cache i dati caricati
def load_data_from_db(query, db_path=DB_PATH):
    """ Carica dati dal database SQLite. """
    if not os.path.exists(db_path):
        st.warning(f"Database {db_path} non trovato. Eseguire prima la pipeline ETL.")
        return pd.DataFrame()
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    return df

# --- Layout dell'App Streamlit ---
st.set_page_config(layout="wide") # Usa l'intera larghezza della pagina
st.title("Dashboard Interattivo: Pipeline Dati Dipendenti")
st.markdown("Visualizza i risultati della pipeline ETL ed esegui nuovamente l'elaborazione.")

# --- Sidebar per Navigazione ed Esecuzione ETL ---
st.sidebar.title("Opzioni")

st.sidebar.header("Esegui Pipeline")
if st.sidebar.button("▶️ Avvia Pipeline ETL Completa"):
    try:
        with st.spinner("Esecuzione della pipeline ETL in corso... Questo potrebbe richiedere del tempo."):
            pipeline = ETLPipelineOrchestrator(
                input_path=config.INPUT_CSV_PATH,
                output_db_path=config.OUTPUT_DB_PATH,
                viz_dir=config.VISUALIZATIONS_DIR
            )
            pipeline.run_full_pipeline() # Questo metodo orchestra extract, transform, load, e reporting
        st.sidebar.success("Pipeline ETL completata con successo!")
        st.sidebar.info(f"Database aggiornato: {config.OUTPUT_DB_PATH}")
        st.sidebar.info(f"Visualizzazioni (file) salvate in: {config.VISUALIZATIONS_DIR}")
        st.balloons()
        # Pulisci la cache per ricaricare i dati aggiornati
        st.cache_data.clear()
        # Aggiorna lo stato dell'esistenza del DB
        global db_exists
        db_exists = os.path.exists(DB_PATH)
        # Ricarica la pagina per riflettere i nuovi dati
        st.rerun() 
    except FileNotFoundError as fnf_error:
        st.sidebar.error(f"Errore File Non Trovato: {fnf_error}. Verifica il percorso del file input.csv in data/ e in config.py.")
    except Exception as e:
        st.sidebar.error(f"Errore critico durante l'esecuzione della pipeline ETL:")
        st.sidebar.exception(e) # Mostra i dettagli dell'eccezione

st.sidebar.markdown("---")
st.sidebar.header("Sezioni Report")
# Definisci le sezioni disponibili solo se il DB esiste
if db_exists:
    analysis_options = [
        "Panoramica Generale",
        "Analisi per Reparto",
        "Analisi per Fascia d'Età",
        "Analisi per Anzianità",
        "Distribuzione Stipendi"
    ]
    choice = st.sidebar.radio("Scegli un'analisi:", analysis_options)
else:
    st.warning("Il database (`data/output.db`) non è stato ancora generato. "
               "Esegui la pipeline ETL tramite il pulsante nella barra laterale per visualizzare i report.")
    choice = None # Nessuna scelta se il DB non esiste

# --- Contenuto Principale Basato sulla Scelta ---

if choice == "Panoramica Generale":
    st.header("📊 Panoramica Generale dei Dipendenti")
    df_dipendenti = load_data_from_db("SELECT * FROM dipendenti")
    if not df_dipendenti.empty:
        st.write(f"Numero totale di dipendenti nel database: **{len(df_dipendenti)}**")
        st.write("Primi record della tabella `dipendenti`:")
        st.dataframe(df_dipendenti.head(), height=300)
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Conteggio Dipendenti per Reparto")
            if config.DEPARTMENT_COLUMN in df_dipendenti.columns:
                counts_reparto = df_dipendenti[config.DEPARTMENT_COLUMN].value_counts()
                st.bar_chart(counts_reparto)
            else:
                st.warning(f"Colonna '{config.DEPARTMENT_COLUMN}' non trovata.")
        
        with col2:
            st.subheader("Conteggio Dipendenti per Fascia d'Età")
            if 'fascia_eta' in df_dipendenti.columns:
                counts_eta = df_dipendenti['fascia_eta'].value_counts().sort_index()
                st.bar_chart(counts_eta)
            else:
                st.warning("Colonna 'fascia_eta' non trovata.")
    else:
        st.info("Nessun dato sui dipendenti trovato. Esegui la pipeline.")


elif choice == "Analisi per Reparto":
    st.header("🏢 Analisi per Reparto")
    # Dati dalla vista analisi_per_reparto
    df_reparto_stats = load_data_from_db("SELECT reparto, numero_dipendenti, stipendio_medio, stipendio_min, stipendio_max, media_anni_servizio, totale_bonus FROM analisi_per_reparto")
    if not df_reparto_stats.empty:
        st.subheader("Statistiche Aggregate per Reparto")
        st.dataframe(df_reparto_stats)

        st.subheader("Distribuzione Stipendi per Reparto (Boxplot)")
        df_dipendenti_sal_dept = load_data_from_db(f"SELECT {config.SALARY_COLUMN}, {config.DEPARTMENT_COLUMN} FROM dipendenti")
        if not df_dipendenti_sal_dept.empty and config.SALARY_COLUMN in df_dipendenti_sal_dept.columns and config.DEPARTMENT_COLUMN in df_dipendenti_sal_dept.columns:
            fig, ax = plt.subplots(figsize=(12, 7))
            df_dipendenti_sal_dept.boxplot(column=config.SALARY_COLUMN, by=config.DEPARTMENT_COLUMN, ax=ax, grid=True)
            ax.set_title('Distribuzione degli Stipendi per Reparto')
            plt.suptitle('') # Rimuove il titolo automatico di pandas
            ax.set_xlabel('Reparto')
            ax.set_ylabel('Stipendio (€)')
            plt.xticks(rotation=45, ha='right')
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.warning("Dati o colonne necessarie per il boxplot non disponibili.")
    else:
        st.info("Nessun dato per l'analisi di reparto. Esegui la pipeline.")

elif choice == "Analisi per Fascia d'Età":
    st.header("🎂 Analisi per Fascia d'Età")
    # Dati dalla vista analisi_per_fascia_eta
    df_eta_stats = load_data_from_db("SELECT fascia_eta, numero_dipendenti, stipendio_medio, stipendio_min, stipendio_max, media_anni_servizio FROM analisi_per_fascia_eta")
    if not df_eta_stats.empty:
        st.subheader("Statistiche Aggregate per Fascia d'Età")
        st.dataframe(df_eta_stats)

        st.subheader("Stipendio Medio per Fascia d'Età")
        if 'fascia_eta' in df_eta_stats.columns and 'stipendio_medio' in df_eta_stats.columns:
            df_plot_eta = df_eta_stats.set_index('fascia_eta')['stipendio_medio']
            st.bar_chart(df_plot_eta)
        else:
            st.warning("Dati necessari per il grafico 'Stipendio Medio per Fascia d'Età' non trovati.")
    else:
        st.info("Nessun dato per l'analisi per fascia d'età. Esegui la pipeline.")


elif choice == "Analisi per Anzianità":
    st.header("📈 Analisi per Anzianità")
    # Dati dalla vista analisi_per_anzianita
    df_anzianita_stats = load_data_from_db("SELECT anzianita, numero_dipendenti, stipendio_medio, stipendio_min, stipendio_max FROM analisi_per_anzianita")
    if not df_anzianita_stats.empty:
        st.subheader("Statistiche Aggregate per Anzianità")
        st.dataframe(df_anzianita_stats)

        st.subheader("Relazione tra Anni di Servizio e Stipendio (Scatter Plot)")
        df_dipendenti_full = load_data_from_db(f"SELECT anni_di_servizio, {config.SALARY_COLUMN}, {config.DEPARTMENT_COLUMN} FROM dipendenti")
        if not df_dipendenti_full.empty and 'anni_di_servizio' in df_dipendenti_full.columns and config.SALARY_COLUMN in df_dipendenti_full.columns:
            fig, ax = plt.subplots(figsize=(10, 7))
            if config.DEPARTMENT_COLUMN in df_dipendenti_full.columns:
                # Crea colori per reparto
                department_categories = df_dipendenti_full[config.DEPARTMENT_COLUMN].astype('category')
                colors = department_categories.cat.codes
                scatter = ax.scatter(df_dipendenti_full['anni_di_servizio'], df_dipendenti_full[config.SALARY_COLUMN], 
                                     alpha=0.6, c=colors, cmap='viridis')
                # Aggiunge legenda per i reparti
                handles, _ = scatter.legend_elements(prop="colors", alpha=0.6)
                if len(handles) == len(department_categories.cat.categories):
                    ax.legend(handles, department_categories.cat.categories, title="Reparto")
            else:
                ax.scatter(df_dipendenti_full['anni_di_servizio'], df_dipendenti_full[config.SALARY_COLUMN], alpha=0.6)
            
            ax.set_title('Relazione tra Anni di Servizio e Stipendio')
            ax.set_xlabel('Anni di Servizio')
            ax.set_ylabel('Stipendio (€)')
            ax.grid(True)
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.warning("Dati necessari per lo scatter plot non disponibili.")
    else:
        st.info("Nessun dato per l'analisi per anzianità. Esegui la pipeline.")


elif choice == "Distribuzione Stipendi":
    st.header("💰 Distribuzione delle Fasce di Stipendio")
    df_dipendenti = load_data_from_db(f"SELECT fascia_stipendio, {config.SALARY_COLUMN} FROM dipendenti") # definisce fascia_stipendio
    if not df_dipendenti.empty and 'fascia_stipendio' in df_dipendenti.columns:
        st.subheader("Conteggio per Fascia di Stipendio (Tabella)")
        stipendio_distribution_table = df_dipendenti['fascia_stipendio'].value_counts().reset_index()
        stipendio_distribution_table.columns = ['Fascia Stipendio', 'Numero Dipendenti']
        st.table(stipendio_distribution_table)

        st.subheader("Distribuzione delle Fasce di Stipendio (Grafico a Torta)")
        fasce_stipendio_counts = df_dipendenti['fascia_stipendio'].value_counts()
        if not fasce_stipendio_counts.empty:
            fig, ax = plt.subplots(figsize=(8,8))
            fasce_stipendio_counts.plot.pie(autopct='%1.1f%%', ax=ax, startangle=90, wedgeprops={'edgecolor': 'black'})
            ax.set_ylabel('') # Nasconde l'etichetta y per i grafici a torta
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.warning("Nessun dato per le fasce di stipendio.")
            
        st.subheader(f"Istogramma della Distribuzione degli Stipendi ({config.SALARY_COLUMN})")
        if config.SALARY_COLUMN in df_dipendenti.columns:
            fig, ax = plt.subplots(figsize=(10,6))
            ax.hist(df_dipendenti[config.SALARY_COLUMN].dropna(), bins=20, edgecolor='black', color='mediumseagreen')
            ax.set_title('Distribuzione degli Stipendi')
            ax.set_xlabel('Stipendio (€)')
            ax.set_ylabel('Frequenza')
            plt.tight_layout()
            st.pyplot(fig)
        else:
            st.warning(f"Colonna '{config.SALARY_COLUMN}' non trovata per l'istogramma.")

    else:
        st.info("Nessun dato sulla distribuzione degli stipendi. Esegui la pipeline.")

# --- Footer ---
st.markdown("---")
st.caption("Progetto Pipeline Dati Dipendenti - Interfaccia Streamlit")
