from flask import render_template, redirect, url_for, Flask
from flask.ext.assets import Environment, Bundle
from flask.ext.sqlalchemy import before_models_committed, event

from sqlalchemy.dialects.postgres import JSONB
from sqlalchemy.sql.expression import cast

app = Flask(__name__)
app.config.from_object('config')

from cms.models import db, Document, on_before_models_committed, on_before_insert
db.init_app(app)

with app.app_context():
    db.create_all()

event.listen(Document, 'before_insert', on_before_insert)
before_models_committed.connect(on_before_models_committed, app)

assets = Environment(app)

style = Bundle('../styles/site.scss', filters='scss,cssmin', output='styles/site.css')
assets.register('style', style)

@app.route('/')
def home():
    things = Document.query.filter(Document.data['type'].astext == 'thing')
    return render_template('home.html', things=things)

if __name__ == '__main__':
    app.run('127.0.0.1', 3030)
