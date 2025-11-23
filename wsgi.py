# wsgi.py
from asgiref.wsgi import AsgiToWsgi   # или: from a2wsgi import ASGIMiddleware
from main import app                  # где объявлен: app = FastAPI()

application = AsgiToWsgi(app)         # если используете a2wsgi: application = ASGIMiddleware(app)
