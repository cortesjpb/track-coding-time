release: superset db upgrade && superset init

web: gunicorn "superset.app:create_app()" 