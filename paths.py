# %% PADRONIZAÇÃO DE PATHS
import os

# Pegar path do diretório atual
CURRENT_DIR = os.getcwd()

# Definir o caminho para a pasta de inputs
INPUTS_PATH = os.path.join(CURRENT_DIR, 'inputs')
OUTPUTS_PATH = os.path.join(CURRENT_DIR, 'outputs')

# Definir o caminho para os arquivos de dados RAW, processados e finais
RAW_DATA_PATH = os.path.join(INPUTS_PATH, 'raw')
PROCESSED_DATA_PATH = os.path.join(INPUTS_PATH, 'processed')
FINAL_DATA_PATH = os.path.join(INPUTS_PATH, 'panels')

# Definir o caminho para a pasta de outputs dos modelos de regressão
REGRESSION_TABLES_PATH = os.path.join(OUTPUTS_PATH, 'tables')
REGRESSION_MODELS_PATH = os.path.join(OUTPUTS_PATH, 'models')
REGRESSION_TESTS_PATH = os.path.join(OUTPUTS_PATH, 'tests')

# Definir o caminho para a pasta de imagens
IMAGES_PATH = os.path.join(CURRENT_DIR, 'img')

# ! Criar as pastas caso não existam
os.makedirs(INPUTS_PATH, exist_ok=True)
os.makedirs(RAW_DATA_PATH, exist_ok=True)
os.makedirs(PROCESSED_DATA_PATH, exist_ok=True)
os.makedirs(FINAL_DATA_PATH, exist_ok=True)
os.makedirs(IMAGES_PATH, exist_ok=True)
os.makedirs(OUTPUTS_PATH, exist_ok=True)
os.makedirs(REGRESSION_TABLES_PATH, exist_ok=True)
os.makedirs(REGRESSION_MODELS_PATH, exist_ok=True)
os.makedirs(REGRESSION_TESTS_PATH, exist_ok=True)
# %%
