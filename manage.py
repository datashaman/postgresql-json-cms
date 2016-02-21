#!/usr/bin/env python

import json

from cms import app
from cms.models import DocumentManager, Document, db
from cms.utils import merge_dicts

from flask.ext.script import Manager
from sqlalchemy import cast
from sqlalchemy.dialects.postgresql import JSONB

manager = Manager(app)

@manager.command
def bootstrap():
    document = DocumentManager.new({
        "_id": "default",
        "type": "layout"
    })
    db.session.add(document)

    document = DocumentManager.update(document, {
        "other": "thing"
    })
    db.session.add(document)

    document = DocumentManager.delete(document)
    db.session.add(document)

    document =  DocumentManager.get(document.id)

    db.session.commit()

@manager.command
def all_current():
    documents = DocumentManager.query_current(type='layout', _deleted=True).all()
    print documents

if __name__ == '__main__':
    manager.run()
