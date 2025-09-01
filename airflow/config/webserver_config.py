import os
from flask import send_from_directory

def configure_app(app):
    @app.route('/eda')
    def eda():
        return send_from_directory(
            directory=os.path.join(app.root_path, 'static'),
            filename='index.html'
        )
