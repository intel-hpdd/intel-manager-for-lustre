FROM emfteam/python-service-base:6.3.0

CMD ["python2", "./manage.py", "chroma_service", "--name=http_agent", "http_agent", "--gevent", "--console"]
