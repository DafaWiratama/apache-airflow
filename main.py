from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive

settings = {
    "client_config_backend": "service",
    "service_config": {"client_json_file_path": "deployment/config/husna-airflow-sa.json"}
}


def get_gdrive_file(folder: str, filename: str, settings: dict):
    gauth = GoogleAuth(settings=settings)
    gauth.ServiceAuth()

    drive = GoogleDrive(gauth)

    statement = {'q': f"'{folder}' in parents and title='{filename}' and trashed=false"}
    files = drive.ListFile(statement).GetList()

    if len(files) == 0:
        return None
    if len(files) > 1:
        raise Exception(f"Multiple files found for folder [{folder}] and filename [{filename}]")
    return files[0]


if __name__ == '__main__':
    file = get_gdrive_file(folder='181R-ug68jYWaI9vBttHnHbt7cTQbuZ1s', filename='2023-10-04.csv', settings=settings)
    print(file)
