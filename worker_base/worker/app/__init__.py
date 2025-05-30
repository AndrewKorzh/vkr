try:
    print("load_dotenv!!!")
    from dotenv import load_dotenv
    from pathlib import Path
    env_path = Path(__file__).resolve().parent.parent / '.env'
    load_dotenv(dotenv_path=env_path)
except Exception as e:
    print("cant load env")
