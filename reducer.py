import json
import redis
from coordinator import REDUCER_COMPLETION_CHANNEL, REDUCER_QUEUE, REDIS_HOST, REDIS_PORT
import time
import logging
import os

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('MapReduce-Reducer')

logger.info(f"Conectando ao Redis em {REDIS_HOST}:{REDIS_PORT}")
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)

def reduce_function(key, values):
    """
    Função reduce para contagem de palavras.
    Recebe uma chave (palavra) e uma lista de valores (contagens), e retorna a soma.
    """
    return sum(values)

def process_reducer_input(input_file, output_file):
    """Processa um arquivo de entrada do reducer usando a função reduce."""
    logger.info(f"Processando entrada do reducer: {input_file}")
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Garante que o diretório de saída existe
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Aplica a função reduce para cada chave e escreve os resultados
        with open(output_file, 'w', encoding='utf-8') as f:
            for key, values in data.items():
                reduced_value = reduce_function(key, values)
                f.write(f"{key}\t{reduced_value}\n")
        
        logger.info(f"Saída do reducer escrita em {output_file} com {len(data)} entradas")
        return True
    
    except Exception as e:
        logger.error(f"Erro processando entrada do reducer {input_file}: {str(e)}")
        return False

def run_reducer():
    """
    Executa o worker reducer que busca tarefas do Redis e as processa.
    """
    logger.info("Worker reducer iniciado")
    
    while True:
        try:
            # Tenta obter uma tarefa da fila com timeout
            task_data = redis_client.blpop(REDUCER_QUEUE, timeout=1)
            
            if task_data:
                _, task_json = task_data
                
                task = json.loads(task_json)
                task_id = task['task_id']
                input_file = task['input_file']
                output_file = task['output_file']
                
                logger.info(f"Tarefa recebida: {task_id} - Processando {input_file}")
                
                # Processa a entrada do reducer
                success = process_reducer_input(input_file, output_file)
                
                if success:
                    redis_client.publish(REDUCER_COMPLETION_CHANNEL, task_id)
                    logger.info(f"Tarefa {task_id} completada com sucesso")
                else:
                    logger.error(f"Tarefa {task_id} falhou")
            
            else:
                logger.debug("Nenhuma tarefa disponível, aguardando...")
        
        except Exception as e:
            logger.error(f"Erro no worker reducer: {str(e)}")
            time.sleep(1)

if __name__ == "__main__":
    run_reducer()