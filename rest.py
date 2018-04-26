import json
import sys
from datetime import datetime

from flask import Flask, request
from flask_sqlalchemy import SQLAlchemy

connect_str = ''
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

    def toJSON(self):
        res = {
            "id": self.id,
            "url": self.url,
            "status": self.status,
            "batch__oid": self.batch__oid
        }
        return res


@app.route('/cdtest/api/v1.0/batch/id/<int:batch_id>', methods=['GET'])
def get_batch_by_id(batch_id):
    res = db.session.query(Batch).get(batch_id)
    res = res.toJSON()
    return json.dumps(res)


@app.route('/cdtest/api/v1.0/batch/file/<string:file_in>', methods=['GET'])
def get_batch_by_file(file_in):
    res = db.session.query(Batch).filter(Batch.file_in.__eq__(file_in)).all()
    json_data = {"batch": []}
    for res_item in res:
        json_item = res_item.toJSON()
        print(json_item)
        json_data["batch"].append(json_item)
    return json.dumps(json_data)

@app.route('/cdtest/api/v1.0/batch/date', methods=['GET'])
def get_batch_by_date():
    date_from = request.args.get('from')
    date_to = request.args.get('to')
    res = db.session.query(Batch).filter(Batch.datetime.between(date_from, date_to))
    json_data = {"batch": []}
    for res_item in res:
        json_item = res_item.toJSON()
        print(json_item)
        json_data["batch"].append(json_item)
    return json.dumps(json_data)


app.run()
