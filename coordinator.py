import json
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

# Redis queues e channels
MAPPER_QUEUE = 'mapper_tasks'
REDUCER_QUEUE = 'reducer_tasks'
MAPPER_COMPLETION_CHANNEL = 'mapper_completion'
REDUCER_COMPLETION_CHANNEL = 'reducer_completion'

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
    
    logger.info(f"Dividindo {DATA_PATH} ({file_size} bytes) em {NUM_MAPPERS} chunks")
    
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

def prepare_mapper_tasks():
    """Prepara tasks de mapper e as coloca na fila Redis."""
    # Limpar fila primeiro
    redis_client.delete(MAPPER_QUEUE)
    
    # Cria task para cada chink
    for i in range(NUM_MAPPERS):
        chunk_file = os.path.join(CHUNK_DIR, f"chunk{i}.txt")
        intermediate_file = os.path.join(INTERMEDIATE_DIR, f"mapper{i}.json")
        
        task = json.dumps({
            'task_id': f"mapper_{i}",
            'input_file': chunk_file,
            'output_file': intermediate_file
        })
        
        # Push task to Redis queue
        redis_client.rpush(MAPPER_QUEUE, task)
        logger.info(f"Adiciona mapper task para a chunk{i}.txt na fila")

def wait_for_mappers():
    """Espera todas as tasks mapper completarem."""
    logger.info(f"Esperando {NUM_MAPPERS} mappers completarem")
    
    # Observa a conclusão de tasks mapper com subscribe ao pubsub
    pubsub = redis_client.pubsub()
    pubsub.subscribe(MAPPER_COMPLETION_CHANNEL)
    
    completos = 0
    
    # espera mensagens de conclusão das tarefas
    while completos < NUM_MAPPERS:
        message = pubsub.get_message(timeout=1.0)
        if message and message['type'] == 'message':
            task_id = message['data']
            logger.info(f"Mapper task Completa: {task_id}")
            completos += 1
    
    pubsub.unsubscribe()
    logger.info("Todas as tasks de mapper concluídas")

def perform_shuffle():
    """Executa a fase de embaralhamento lendo as saídas do mapeador e agrupando por chaves."""
    logger.info("Começando o shuffle")
    all_key_values = {}
    
    # Ler arquivos intermediários dos mappers
    for i in range(NUM_MAPPERS):
        intermediate_file = os.path.join(INTERMEDIATE_DIR, f"mapper{i}.json")
        try:
            with open(intermediate_file, 'r', encoding='utf-8') as f:
                mapper_output = json.load(f)
                for key, value in mapper_output.items():
                    if key not in all_key_values:
                        all_key_values[key] = []
                    all_key_values[key].extend(value)
        except FileNotFoundError:
            logger.warning(f"Arquivo intermediário não encontrado: {intermediate_file}")
    
    # Particionar para reducers
    reducer_partitions = [{} for _ in range(NUM_REDUCERS)]
    for key, values in all_key_values.items():
        reducer_index = int(hashlib.md5(key.encode()).hexdigest(), 16) % NUM_REDUCERS
        reducer_partitions[reducer_index][key] = values
    
    # Salvar arquivos de entrada dos reducers
    for i in range(NUM_REDUCERS):
        reducer_input_file = os.path.join(REDUCER_INPUT_DIR, f"reducer{i}_input.json")
        with open(reducer_input_file, 'w', encoding='utf-8') as f:
            json.dump(reducer_partitions[i], f)
    
    logger.info("Shuffle concluído")

def prepare_reducer_tasks():
    """Prepara tarefas para os reducers e as envia para a fila do Redis."""
    redis_client.delete(REDUCER_QUEUE)
    
    # Cria uma tarefa para cada reducer
    for i in range(NUM_REDUCERS):
        input_file = os.path.join(REDUCER_INPUT_DIR, f"reducer{i}_input.json")
        output_file = os.path.join(REDUCER_OUTPUT_DIR, f"reducer{i}_output.txt")
        
        task = json.dumps({
            'task_id': f"reducer_{i}",
            'input_file': input_file,
            'output_file': output_file
        })
        
        # Adiciona a tarefa na fila do Redis
        redis_client.rpush(REDUCER_QUEUE, task)
        logger.info(f"Tarefa do reducer{i} adicionada à fila")

def wait_for_reducers():
    """Aguarda todas as tarefas dos reducers serem completadas."""
    logger.info(f"Aguardando {NUM_REDUCERS} reducers completarem")
    
    pubsub = redis_client.pubsub()
    pubsub.subscribe(REDUCER_COMPLETION_CHANNEL)
    
    completos = 0
    
    # Aguarda mensagens de conclusão
    while completos < NUM_REDUCERS:
        message = pubsub.get_message(timeout=1.0)
        if message and message['type'] == 'message':
            task_id = message['data']
            logger.info(f"Tarefa do reducer concluída: {task_id}")
            completos += 1
    
    pubsub.unsubscribe()
    logger.info("Todas as tarefas dos reducers foram concluídas")

def merge_results():
    """Combina todos os resultados dos reducers em um arquivo final."""
    logger.info(f"Combinando saídas dos reducers em {FINAL_RESULT_DIR}/{FINAL_RESULT}")
        
    resultados_combinados = {}
    
    for i in range(NUM_REDUCERS):
        arquivo_saida = os.path.join(REDUCER_OUTPUT_DIR, f"reducer{i}_output.txt")
        
        try:
            with open(arquivo_saida, 'r', encoding='utf-8') as f:
                for linha in f:
                    linha = linha.strip()
                    if linha:
                        chave, valor = linha.split('\t')
                        resultados_combinados[chave] = int(valor)
        except FileNotFoundError:
            logger.warning(f"Arquivo de saída do reducer não encontrado: {arquivo_saida}")
    
    # Ordena por valor (decrescente)
    resultados_ordenados = sorted(resultados_combinados.items(), key=lambda x: x[1], reverse=True)
    
    # Escreve o resultado final
    arquivo_final = os.path.join(FINAL_RESULT_DIR, FINAL_RESULT)
    with open(arquivo_final, 'w', encoding='utf-8') as f:
        for chave, valor in resultados_ordenados:
            f.write(f"{chave}\t{valor}\n")
    
    logger.info(f"Resultado final gravado em {FINAL_RESULT} com {len(resultados_combinados)} entradas")

def run_mapreduce():
    """Executa o processo MapReduce."""
    logger.info("Iniciando o processo MapReduce")
    # setup
    create_directories()
    # passo 1: divisao do arquivo de entrada em pedaços
    split_input_file()
    
    # passo 2: As tarefas de mapping são enviadas para uma fila no Redis
    prepare_mapper_tasks()
    logger.info("Mappers serão executados agora")
    wait_for_mappers()

    # passo 3: Shuffle fase
    perform_shuffle()

    #passo 4: Preparar e executar tarefas do reduce

    prepare_reducer_tasks()
    logger.info("Reducers serão executados agora")
    wait_for_reducers()

    #passo5: Resultados do merge
    merge_results()
    logger.info("MapReduce concluído com sucesso")
    
    # Os workers de mapping retiram tarefas da fila, processam-nas 
    # e emitem pares chave-valor intermediários
    wait_for_mappers()
    
if __name__ == "__main__":
    run_mapreduce()