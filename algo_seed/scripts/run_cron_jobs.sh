
#!/bin/bash

# Activate the virtual environment
source /home/ec2-user/algo_seed/venv/bin/activate

# Run the required scripts (add your scripts here)
python /home/ec2-user/algo_seed/scripts/input_algo_seed.py 1toWzsO7YhM_7pqi2PBof5n00LFh_G6S0CHw4v2AY9qU OUTPUT input_algo_seed
python /home/ec2-user/algo_seed/scripts/input_algo_seed.py 1toWzsO7YhM_7pqi2PBof5n00LFh_G6S0CHw4v2AY9qU RESEED_OUTPUT input_algo_re_seed
python /home/ec2-user/algo_seed/scripts/input_algo_seed.py 1toWzsO7YhM_7pqi2PBof5n00LFh_G6S0CHw4v2AY9qU EXTRA_OUTPUT input_algo_extra_seed
python /home/ec2-user/algo_seed/scripts/raw_algo_seed.py
python /home/ec2-user/algo_seed/scripts/stg_algo_seed_hl.py
python /home/ec2-user/algo_seed/scripts/stg_algo_seed.py 20
python /home/ec2-user/algo_seed/scripts/stg_algo_seed.py 50
python /home/ec2-user/algo_seed/scripts/stg_algo_seed.py 100
python /home/ec2-user/algo_seed/scripts/rank_algo_seed.py --input_table stg_algo_seed_020
python /home/ec2-user/algo_seed/scripts/rank_algo_seed.py --input_table stg_algo_seed_050
python /home/ec2-user/algo_seed/scripts/rank_algo_seed.py --input_table stg_algo_seed_100

# Deactivate the virtual environment (optional)
deactivate
