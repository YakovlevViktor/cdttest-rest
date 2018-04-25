import sys
import json
from datetime import datetime

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

if len(sys.argv) != 2:
    print("Usage: python %s %s" % (sys.argv[0], "db_connect_string"))
else:
    connect_str = sys.argv[1]

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "%s%s" % ('postgresql://', connect_str)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


class Batch(db.Model):
    id = db.Column('id', db.Integer, primary_key=True)
    file_in = db.Column('file_in', db.String)
    file_out = db.Column('file_out', db.String)
    datetime = db.Column('datetime', db.DateTime, default=datetime.now())
    status = db.Column('status', db.String, default='OK')

    def toJSON(self):
        res = {
            "id": self.id,
            "file_in": self.file_in,
            "file_out": self.file_out,
            "datetime": str(self.datetime),
            "status": self.status
        }
        return res


class Task(db.Model):
    id = db.Column('id', db.Integer, primary_key=True)
    url = db.Column('url', db.String)
    status = db.Column('status', db.String)
    batch__oid = db.Column('batch__oid', db.Integer, db.ForeignKey('batch.id'), nullable=False)
    batch = db.relationship('Batch', backref=db.backref('tasks', lazy=True))


@app.route('/cdtest/api/v1.0/batch/<int:batch_id>', methods=['GET'])
def get_batch(batch_id):
    res = db.session.query(Batch).get(batch_id)
    res = res.toJSON()
    return json.dumps(res)
    # res = json.dumps(res)
    # return res


app.run()
