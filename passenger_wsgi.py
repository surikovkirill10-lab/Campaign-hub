# passenger_wsgi.py
import os, sys

# необязательно, но полезно явно добавить путь проекта:
# sys.path.insert(0, os.path.dirname(__file__))

from wsgi import application  # <- WSGI callable, который мы отдали в wsgi.py
