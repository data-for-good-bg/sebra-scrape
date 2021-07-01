import os
import dotenv

dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
dotenv.load_dotenv(dotenv_path)


PROJECT_DB = os.environ.get("PROJECT_DB")
USR = os.environ.get("USR")
PASS = os.environ.get("PASS")
HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")


postgres_proj_str = f'postgresql://{USR}:{PASS}@{HOST}:{PORT}/{PROJECT_DB}'
