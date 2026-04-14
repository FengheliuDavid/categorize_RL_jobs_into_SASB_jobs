#!/bin/bash
#SBATCH --partition=gpunormal
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --gres=gpu:3
#SBATCH --time=24:00:00
#SBATCH --mem=150G
#SBATCH --job-name=rl_desc_recovery
#SBATCH --output=Outputs/desc_%j_%N.out
#SBATCH --error=Outputs/desc_%j_%N.err
#SBATCH --nodelist=c011

WORKDIR=/gpfs/home/fl488/process_RL_sasb/gemma_description_classification

mkdir -p $WORKDIR/Outputs
cd $WORKDIR

export PATH=/gpfs/project/populism/ollama/bin:$PATH

module load python/3.11.11-wepq
source /gpfs/project/populism/venv/bin/activate

cleanup(){
  echo "Cleaning up on $(hostname)..."
  kill $P0 $P1 $P2 $MON_PID &>/dev/null || true
}
trap cleanup EXIT SIGINT SIGTERM

srun \
  --output=$WORKDIR/Outputs/desc_srun_%j_%N.out \
  --error=$WORKDIR/Outputs/desc_srun_%j_%N.err \
  bash -lc '

  export PATH=/gpfs/project/populism/ollama/bin:$PATH
  WORKDIR=/gpfs/home/fl488/process_RL_sasb/gemma_description_classification

  #-- GPU monitoring --#
  (
    while true; do
      echo "----- $(date) GPU USAGE on $(hostname) -----"
      nvidia-smi
      sleep 120
    done
  ) > $WORKDIR/Outputs/gpu_usage_$(hostname).log 2>&1 & MON_PID=$!

  #-- Ollama servers on GPUs 0,1,2 --#
  CUDA_VISIBLE_DEVICES=0 OLLAMA_HOST=127.0.0.1:11434 \
    ollama start > $WORKDIR/Outputs/ollama_service_gpu0_$(hostname).log 2>&1 & P0=$!
  CUDA_VISIBLE_DEVICES=1 OLLAMA_HOST=127.0.0.1:11435 \
    ollama start > $WORKDIR/Outputs/ollama_service_gpu1_$(hostname).log 2>&1 & P1=$!
  CUDA_VISIBLE_DEVICES=2 OLLAMA_HOST=127.0.0.1:11436 \
    ollama start > $WORKDIR/Outputs/ollama_service_gpu2_$(hostname).log 2>&1 & P2=$!

  #-- Wait for all 3 to be up --#
  for port in 11434 11435 11436; do
    until curl -s http://127.0.0.1:$port/ | grep -q "Ollama is running"; do
      sleep 1
    done
  done

  #-- Pull model on this node --#
  echo "Pulling gemma3:27b on $(hostname)..."
  ollama pull gemma3:27b
  echo "Model ready on $(hostname)."

  echo "Running recovery shard 0 of 5 on $(hostname)..."

  /gpfs/project/populism/venv/bin/python $WORKDIR/gemma_description_classification.py \
    --input-csv /gpfs/home/fl488/process_RL_sasb/gemma_role_classification/comparison_sasb.csv \
    --output-csv /gpfs/home/fl488/process_RL_sasb/gemma_description_classification/gemma_description_classification_output.csv \
    --shard-id=0 \
    --num-shards=5 \
    --max-concurrent=20
'
