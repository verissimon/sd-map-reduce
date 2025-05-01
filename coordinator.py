import os
from generate_test_data import generate_test_data
import redis
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('MapReduce-Coordinator')

REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
NUM_MAPPERS = 10
NUM_REDUCERS = 5
DATA_DIR = 'data'
DATA_FILE = 'data.txt'
DATA_PATH = f"{DATA_DIR}/{DATA_FILE}"
CHUNK_DIR = 'chunks'
INTERMEDIATE_DIR = 'intermediate'
REDUCER_INPUT_DIR = 'reducer_input'
REDUCER_OUTPUT_DIR = 'reducer_output'
FINAL_RESULT_DIR = 'final_result'
FINAL_RESULT = 'final_result.txt'

logger.info(f"Conectando ao redis em {REDIS_HOST}:{REDIS_PORT}")
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def create_directories():
    """Cria arquivos e diretórios necessários."""
    directories = [CHUNK_DIR, INTERMEDIATE_DIR, REDUCER_INPUT_DIR, REDUCER_OUTPUT_DIR, FINAL_RESULT_DIR]
    for directory in directories:
        os.makedirs(directory, exist_ok=True)
    
    # Se não há data.txt, ele será gerado
    if not os.path.exists(DATA_PATH):
        generate_test_data(DATA_PATH, size_mb=10)
        
    # Cria arquivo de resultado final vazio
    try:
        with open(f"./{FINAL_RESULT_DIR}/{FINAL_RESULT}", 'w', encoding='utf-8'):
            pass 
    except OSError as e:
        logger.error(f"Erro criando o arquivo de resultados {FINAL_RESULT}: {e}")
        raise

def split_input_file():
    """Divide o arquivo de entrada em pedaços de tamanho aproximadamente igual."""
    file_size = os.path.getsize(DATA_PATH)
    chunk_size = file_size // NUM_MAPPERS
    
    logger.info(f"Splitting {DATA_PATH} ({file_size} bytes) into {NUM_MAPPERS} chunks")
    
    with open(DATA_PATH, 'r', encoding='utf-8') as f:
        chunk_number = 0
        bytes_read = 0
        
        current_chunk = []
        
        for line in f:
            current_chunk.append(line)
            bytes_read += len(line.encode('utf-8'))
            
            if bytes_read >= chunk_size and chunk_number < NUM_MAPPERS - 1:
                write_chunk(current_chunk, chunk_number)
                current_chunk = []
                bytes_read = 0
                chunk_number += 1
        
        # Escreve último chunk
        if current_chunk:
            write_chunk(current_chunk, chunk_number)
    
    logger.info(f"Divisão de data.txt em {NUM_MAPPERS} chunks finalizou com sucesso.")

def write_chunk(chunk_data, chunk_number):
    """Escreve um chunk de data.txt em um arquivo."""
    chunk_file = os.path.join(CHUNK_DIR, f"chunk{chunk_number}.txt")
    with open(chunk_file, 'w', encoding='utf-8') as f:
        f.writelines(chunk_data)
    logger.info(f"Chunk criado: {chunk_file}")

def run_mapreduce():
    """Executa o processo MapReduce."""
    logger.info("Iniciando o processo MapReduce")
    
    create_directories()
    
    split_input_file()
    
if __name__ == "__main__":
    run_mapreduce()