# Sistema Distribuído de MapReduce com Redis

Este projeto implementa um sistema distribuído de MapReduce que utiliza o Redis para coordenação de tarefas e troca de mensagens. O sistema é projetado para processar grandes arquivos de texto de forma distribuída, utilizando múltiplos workers em paralelo.

## Arquitetura do Sistema

O sistema consiste nos seguintes componentes:

1. **Coordinator**: Orquestra todo o processo de MapReduce, incluindo a divisão dos dados de entrada, atribuição de tarefas aos trabalhadores e fusão dos resultados finais.
2. **Mapper Workers**: Processam partes dos dados de entrada e emitem pares chave-valor.
3. **Fase de Shuffle**: Agrupa valores por chave e os particiona para os reducers.
4. **Reducer Workers**: Agregam os valores de cada chave para produzir os resultados finais.
5. **Redis**: Utilizado para enfileiramento de tarefas e coordenação dos workers.

## Como Funciona

1. O script coordinator divide os dados de entrada em partes.
2. As tarefas de mapping são enviadas para uma fila no Redis.
3. Os workers de mapping retiram tarefas da fila, processam-nas e emitem pares chave-valor intermediários.
4. A fase de shuffle agrupa valores por chave e os particiona em tarefas de reduce.
5. As tarefas de reduce são enviadas para uma fila no Redis.
6. Os workers de reduce retiram tarefas, processam-nas e escrevem a saída final.
7. O script coordinator mescla todas as saídas dos redutores em um arquivo de resultado final.

## Componentes

### Coordinator (`coordinator.py`)
- Gerencia o fluxo de trabalho geral do MapReduce
- Divide os dados de entrada em partes
- Envia tarefas para as filas do Redis
- Realiza a operação de shuffle
- Mescla as saídas dos reducers no resultado final

### Mapper (`mapper.py`)
- Busca tarefas de map na fila do Redis
- Processa partes de entrada
- Aplica a função de map (contagem de palavras neste exemplo)
- Escreve resultados intermediários
- Sinaliza a conclusão via pub/sub do Redis

### Redutor (`reducer.py`)
- Busca tarefas de reduce na fila do Redis
- Processa pares chave-valor agrupados
- Aplica a função reduce (soma neste exemplo)
- Escreve a saída final para sua partição
- Sinaliza a conclusão via pub/sub do Redis

### Gerador de Dados de Teste (`generate_test_data.py`)
- Gera dados de texto de exemplo para teste
- Cria um arquivo de tamanho configurável

## Requisitos

- Python 3.9+
- Redis
- Pacotes Python necessários: `redis`

## Executando o Sistema

### Usando Docker Compose

A maneira mais fácil de executar o sistema é com o Docker Compose:

```bash
docker-compose up --build
```

Isso iniciará:
- Servidor Redis
- Serviço do Coordinator
- Múltiplos workers de map
- Múltiplos workers de reduce

### Configuração Manual

1. Caso seu sistema operacional seja Windows, utilize WSL2. Siga o guia oficial para instalar o WSL: [Guia de Instalação do WSL](https://learn.microsoft.com/en-us/windows/wsl/install)

2. Instale o Redis Server: [Guia de Instalação do Redis](https://redis.io/docs/latest/operate/oss_and_stack/install/archive/install-redis/)

3. Instale o Python e o pip:  
    ```bash
    sudo apt update
    sudo apt install -y python3 python3-pip
    ```

4. Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

5. Copie seus dados para processamento:  
    Essa etapa pode ser ignorada, e um arquivo de teste será gerado e processado ao invés dos seus dados. 
    ```bash
    cp seus_dados.txt ./data/data.txt
    ```

6. Execute o script bash para executar o fluxo de trabalho completo:
    ```bash
    chmod +x run.sh
    ./run.sh
    ```
    Esse processo irá gerar arquivos de log no diretório `logs`, detalhando o fluxo de execução do MapReduce.

    O arquivo com os resultados estará no diretório `final_result` 

## Personalização

### Funções de Map e Reduce

Você pode personalizar as funções de map e reduce em `mapper.py` e `reducer.py`, respectivamente:

- **Função de Map**: Edite a função `map_function()` em `mapper.py`
- **Função de Reduce**: Edite a função `reduce_function()` em `reducer.py`

### Configuração

Ajuste os seguintes parâmetros em `coordinator.py`:

- `NUM_MAPPERS`: Número de partes em que os dados de entrada serão divididos
- `NUM_REDUCERS`: Número de tarefas de redução

Ajuste também o número de instâncias dos containeres em `docker-compose.yml`, com o número desejado na propriedade `replicas`

Caso queira executar localmente, edite também a quantidade instâncias de mappers e reducers no script `run.sh`:
Aumente o número de iterações do for para mais mappers e reducers
```bash
# run.sh
echo "Iniciando mapper workers..."
for i in {0..9}; do # 10 mappers
    python3 mapper.py > ./logs/mapper_$i.log 2>&1 &
    MAPPER_PIDS[$i]=$!
done
```

```bash
# run.sh
echo "Iniciando reducer workers..."
for i in {0..4}; do # 5 reducers
    python3 reducer.py > ./logs/reducer_$i.log 2>&1 &
    REDUCER_PIDS[$i]=$!
done
```

## Estrutura de Diretórios

- `chunks/` - Contém as partes dos dados de entrada divididos
- `intermediate/` - Contém as saídas intermediárias dos mapeadores
- `reducer_input/` - Contém os dados embaralhados para os redutores
- `reducer_output/` - Contém as saídas individuais dos redutores
- `final_result/final_result.txt` - O resultado final mesclado

## Considerações de Desempenho

- Aumente o número de réplicas de mapeadores e redutores em `docker-compose.yml` para maior paralelismo.
- Para arquivos grandes, os processos de divisão em chunks e shuffle de arquivos intermediários serão demorados, pois apenas os mappers e reducers trabalham em paralelo.
