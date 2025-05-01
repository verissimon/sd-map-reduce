#!/bin/bash

if ! pgrep redis-server > /dev/null; then
    echo "Iniciando o Redis server..."
    redis-server --daemonize yes
fi

mkdir -p logs

echo "Iniciando coordinator..."
python3 coordinator.py > ./logs/coordinator.log 2>&1 &
COORDINATOR_PID=$!

# Espera inicializar
sleep 2

echo "Iniciando mapper workers..."
for i in {0..9}; do # 10 mappers
    python3 mapper.py > ./logs/mapper_$i.log 2>&1 &
    MAPPER_PIDS[$i]=$!
done

# monitora logs do coordinador para verificar se todas as tasks de mapper foram concluídas
while true; do
    if grep -q "Todas as tasks de mapper concluídas" ./logs/coordinator.log; then
        echo "Mapping concluído!"
        break
    fi
    sleep 1
done

echo "Iniciando reducer workers..."
for i in {0..4}; do # 5 reducers
    python3 reducer.py > ./logs/reducer_$i.log 2>&1 &
    REDUCER_PIDS[$i]=$!
done

echo "Esperando o processo MapReduce terminar..."
while true; do
    if grep -q "MapReduce concluído com sucesso" ./logs/coordinator.log; then
        echo "MapReduce concluído com sucesso!"
        break
    fi
    sleep 1
done

echo "Limpando processos..."
kill $COORDINATOR_PID
for pid in "${MAPPER_PIDS[@]}"; do
    kill $pid 2>/dev/null
done
for pid in "${REDUCER_PIDS[@]}"; do
    kill $pid 2>/dev/null
done

echo "Resultados em final_result.txt"
