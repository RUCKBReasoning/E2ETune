The source code for E2ETune

## Environment Installation

1. Preparations: Python == 3.7

2. Install packages

   ```
   pip install -r requirements.txt
   ```

3. Download and install PostgreSQL 12.2

   ```
   sudo apt-get update
   sudo apt-get install postgresql postgresql-client
   ```

## Training Data Construction

### Workload Preparation

+ **OLAP Workloads**: Our generated OLAP workloads are under `olap_workloads`fold, one can replay them by execute the files on PG.

+ **OLTP Workloads**: Install `benchbase` to execute OLTP workloads, refer to https://github.com/cmu-db/benchbase.

  ```bash
  git clone --depth 1 https://github.com/cmu-db/benchbase.git
  cd benchbase
  ./mvnw clean package -P postgres
  ```

  This produces artifacts in the `target` folder, which can be extracted,

  ```bash
  cd target
  tar xvzf benchbase-postgres.tgz
  cd benchbase-postgres
  ```

  Inside this folder, you can run BenchBase. For example, to execute the `tpcc` benchmark,

  ```bash
  java -jar benchbase.jar -b tpcc -c config/postgres/sample_tpcc_config.xml --create=true --load=true --execute=true
  ```

  A full list of options can be displayed,

  ```bash
  java -jar benchbase.jar -h
  ```

  We realise our generated OLTP workloads in `oltp_workloads` fold by configuration files of `benchbase`, one can move the fold into `benchbase/target/benchbase-postgres/config/postgres` and execute them as follows (take `tpcc` as an example):

  ```bash
  cd /your/path/benchbase/target/benchbase-postgres
  java -jar benchbase.jar -b tpcc -c config/postgres/sample_tpcc_config0.xml --clear=true --create=true --load=true --execute=true --directory /your/results/path
  ```

### Label Collection

Collect optimal configuration under your environment by HEBO method as follow

1. Fill the Configuration File `config/config.ini`, such as the database and host configuration, as follows.

   ```
   [database_config]
   host = localhost
   port = 5432
   user = 
   password = 
   database = 
   data_path = 
   
   [ssh_config]
   host = 
   port = 
   user = 
   password = 
   ```

2. Run the workload tuning process as follows

   ```bash
   # Run OLAP workload
   python main.py --workload tpch_0.wg --host localhost --database tpch --datapath /your/DB/data/path --method HEBO
   # Run OLTP workload
   python main.py --workload sample_tpcc_config0.xml  --host localhost --database tpcc --datapath /your/DB/data/path --method HEBO
   ```

## Fine-tuning LM

### Prepare the data

Change the data format to 

```bash
bash post_process/run.sh
```

### Fine-tuning

Install following https://github.com/hiyouga/LLaMA-Factory and fine-tune LM as follow

```bash
deepspeed --include localhost:0,1,2,3,4,5,6,7 --master_port=12333 src/train.py \
    --deepspeed ds_config.json \
    --stage sft \
    --model_name_or_path /nvme/shared_ckpt/mistral-7b-instruct-v0.2 \
    --do_train \
    --dataset PO \
    --template mistral \
    --finetuning_type full \
    --output_dir /nvme/yzh/LLaMA-Factory/random \
    --overwrite_cache \
    --per_device_train_batch_size 1 \
    --gradient_accumulation_steps 4 \
    --lr_scheduler_type cosine \
    --report_to wandb \
    --logging_steps 1 \
    --save_strategy 'epoch' \
    --learning_rate 2e-5 \
    --num_train_epochs 10.0 \
    --plot_loss \
    --max_length 8192 \
    --cutoff_len 8192 \
    --bf16

```

## Knob Tuning via Fine-tuned LM

Restore the inference results of LM as the following format

```json
 [
 		{"database": "tpch",
     "workload": "twitter/sample_twitter_config.xml",
     "instruction": "You are an expert in database, you are to optimize the parameters of database, please output in json format, for each field, output one of \"00% to 10%\", \"10% to 20%\", \"20% to 30%\", \"40% to 50%\", \"50% to 60%\", \"60% to 70%\", \"70% to 80%\", \"80% to 90%\", \"90% to 100%\"",
     "input": "workload features: size of workload: 15.0; read ratio: 1.0; group by ratio: 0.8; order by ratio: 0.87; .... ",
     "model_outputs": [
            {"max_wal_senders": "40% to 50%", "autovacuum_max_workers": "40% to 50%"....},
            {"max_wal_senders": "10% to 20%", ....},
            {"max_wal_senders": "50% to 60%", ....},
            {"max_wal_senders": "30% to 40%", ....},
            {"max_wal_senders": "80% to 90%", ....},
            {"max_wal_senders": "90% to 100%", ....}
        ]
    },
    
]   
```

Then run the `model_output_test.py` to evaluate the configurations recommended by LM by executing on actual database

``` bash
python model_output_test.py --host localhost --datapath /your/DB/data/path
```

