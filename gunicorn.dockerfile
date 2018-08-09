FROM imlteam/manager-service-base:extract-nginx

ENTRYPOINT ["gunicorn", "chroma-manager:application", "-c", "./chroma-manager.py", "-e", "USE_CONSOLE=1", "--preload"]
