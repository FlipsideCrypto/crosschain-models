import yaml
import argparse
import os
import subprocess
import traceback
import shlex

def get_dbt_profile(target):
    """
    Retrieve Snowflake connection details from the local dbt profile.
    """
    try:
        output = subprocess.check_output(['dbt', 'debug']).decode('utf-8')
        profiles_dir = None
        for line in output.splitlines():
            if 'Using profiles.yml file at' in line:
                profiles_dir = os.path.dirname(line.replace('Using profiles.yml file at', '').strip())
                break
        if not profiles_dir:
            raise ValueError("DBT_PROFILES_DIR not found in dbt debug output")

        with open("dbt_project.yml", 'r') as f:
            dbt_config = yaml.safe_load(f)
            profile_name = dbt_config.get('profile')
            if not profile_name:
                raise ValueError("Profile not found in dbt_project.yml")
            
        with open(os.path.join(profiles_dir, 'profiles.yml'), 'r') as f:
            profiles = yaml.safe_load(f)
            
        config = profiles[profile_name]['outputs'][target]

        return config

    except Exception as e:
        print(f"Error in get_dbt_profile function: {e}")
        print(traceback.format_exc())
        raise

def upload_file_to_stage(filepath, config):
    """
    Upload a file to Snowflake stage using snowsql PUT command.
    """
    account = config['account']
    user = config['user']
    authenticator = config['authenticator']
    role = config.get('role', '')
    warehouse = config.get('warehouse', '')
    database = config['database']
    schema = config['schema']

    cmd = (f"snowsql -a {account} -u {user} --authenticator {authenticator} "
           f"-r {role} -w {warehouse} -d {database} -s {schema} "
           f"-q \"PUT file://{shlex.quote(filepath)} @json_config_stage\"")

    try:
        subprocess.run(cmd, shell=True, check=True)
    except subprocess.CalledProcessError as e:
        print(f"Error uploading file {filepath} to Snowflake stage: {e}")
        print(traceback.format_exc())
        raise

if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(description='Upload JSON file to Snowflake stage.')
        parser.add_argument('--config_file', required=True, help='Path to the JSON file to upload.')
        parser.add_argument('--target', default='dev', help='Target environment for dbt (default: dev).')
        args = parser.parse_args()

        snowflake_config = get_dbt_profile(args.target)
        upload_file_to_stage(args.config_file, snowflake_config)

    except Exception as e:
        print(f"An error occurred: {e}")
        print(traceback.format_exc())
        raise