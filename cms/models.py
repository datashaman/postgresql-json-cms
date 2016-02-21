import md5

from cms.utils import merge_dicts
from flask.ext.sqlalchemy import Model, SQLAlchemy
from sqlalchemy import text, CheckConstraint, cast
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import backref
from sqlalchemy.schema import Index, MetaData
from uuid import uuid4

class IntegrityError(Exception):
    pass


convention = {
    "ix": 'ix_%(column_0_label)s',
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s"
}

metadata = MetaData(naming_convention=convention)
db = SQLAlchemy(metadata=metadata)

class DocumentManager(object):
    @classmethod
    def new(cls, data):
        data['_deleted'] = False
        document = Document(id=data.get('_id', str(uuid4())),
                            version=1,
                            hash=cls.create_hash(data),
                            data=data,
                            current=True)

        return document

    @classmethod
    def get(cls, id):
        document = Document.query.filter_by(id=id, current=True).first()
        return document

    @classmethod
    def query_current(cls, **properties):
        filters = [ Document.current == True ]

        for k,v in properties.iteritems():
            filters.append(Document.data[k] == cast(v, JSONB))

        documents = Document.query.filter(*filters)

        return documents

    @classmethod
    def create_hash(cls, data):
        return md5.new('%s%d' % (data, data.get('_deleted', False))).hexdigest()

    @classmethod
    def update(cls, existing, properties):
        if not existing.current:
            raise IntegrityError('may only update current revision')

        existing.current = False

        data = merge_dicts(existing.data, properties)

        document = Document(id=existing.id,
                            version=existing.version+1,
                            hash=cls.create_hash(data),
                            data=data,
                            current=True)

        return document

    @classmethod
    def delete(cls, existing):
        return cls.update(existing, { '_deleted': True })

class Document(db.Model):
    __tablename__ = 'documents'

    id = db.Column(db.String, primary_key=True)
    version = db.Column(db.BigInteger, nullable=False, autoincrement=False, primary_key=True)
    hash = db.Column(db.String, nullable=False, primary_key=True)

    current = db.Column(db.Boolean, nullable=False)

    data = db.Column(JSONB)

    CheckConstraint(data['_id'].astext != None, name='id_not_null')
    CheckConstraint(data['_version'].astext != None, name='version_not_null')
    CheckConstraint(data['_hash'].astext != None, name='hash_not_null')
    CheckConstraint(data['type'].astext != None, name='type_not_null')

    Index('ix_type', data['type'].astext)

    def __repr__(self):
        return '%d-%s' % (self.version, self.hash)

def on_before_insert(mapper, connection, instance):
    instance.data['_id'] = instance.id
    instance.data['_version'] = instance.version
    instance.data['_hash'] = instance.hash

def on_before_models_committed(sender, changes):
    sender.logger.debug('checking data')

    for instance, operation in changes:
        if operation == 'delete':
            raise IntegrityError('delete not allowed')
