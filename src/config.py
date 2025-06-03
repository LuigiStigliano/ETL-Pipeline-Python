import os

# Directory base del progetto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Percorsi dei dati
INPUT_CSV_PATH = os.path.join(BASE_DIR, 'data', 'input.csv')
OUTPUT_DB_PATH = os.path.join(BASE_DIR, 'data', 'output.db')
VISUALIZATIONS_DIR = os.path.join(BASE_DIR, 'data', 'visualizzazioni')

# Colonne per la rimozione dei duplicati (tutte tranne 'id')
DUPLICATE_CHECK_COLUMNS = [
    'nome', 'cognome', 'eta', 'stipendio',
    'data_assunzione', 'reparto'
]

# Colonne per gestione valori mancanti
SALARY_COLUMN = 'stipendio'
HIRE_DATE_COLUMN = 'data_assunzione'
DEPARTMENT_COLUMN = 'reparto'
AGE_COLUMN = 'eta'

# Parametri per la trasformazione
AGE_BINS = [0, 30, 40, 50, float('inf')]
AGE_LABELS = ['20-30', '31-40', '41-50', '50+']

SALARY_BINS = [0, 40000, 50000, float('inf')]
SALARY_LABELS = ['Basso', 'Medio', 'Alto']

SENIORITY_BINS = [-1, 2, 5, 8, float('inf')]
SENIORITY_LABELS = ['Junior', 'Mid', 'Senior', 'Expert']

HOURS_PER_WEEK = 40
WEEKS_PER_YEAR = 52