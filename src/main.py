
import yaml
import argparse

from pipeline.my_pipeline import run_pipeline

def load_config(config_file):
    with open(config_file, 'r') as cf:
        return yaml.safe_load(cf)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--config",
        "-C",
        default="config/dev_config.yaml",
        help="Path to config file"
    )
    
    args, beam_args = parser.parse_known_args()
    
    config_data = load_config(args.config)
    
    run_pipeline(config_data, beam_args)
    
    