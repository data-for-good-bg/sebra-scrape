import os
import dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)


PROJECT_DB = os.environ.get("PROJECT_DB")
USR = os.environ.get("USR")
PASS = os.environ.get("PASS")
HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")
main_project_drive_id = os.environ.get("main_project_drive_id")
raw_data_folder_id = os.environ.get("raw_data_folder_id")
parsed_data_folder_id = os.environ.get("parsed_data_folder_id")
manually_fixed_drive_id = os.environ.get("manually_fixed_drive_id")
unparsable_files_drive_id = os.environ.get("unparsable_files_drive_id")


postgres_proj_str = f'postgresql://{USR}:{PASS}@{HOST}:{PORT}/{PROJECT_DB}'
