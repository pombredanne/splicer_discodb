import os
from nose.tools import *

from splicer.ast import LoadOp
from  splicer import DataSet, Query
from splicer_discodb import create, DiscoDBServer



class TestDiscoDBServer(object):
  def setup(self):
    self.db = create(
      "/tmp/data.db",
      schema=dict(
        fields=[
          dict(
            name="name",
            type="STRING"
          ),
          dict(
            name="login_count",
            type="INTEGER"
          )

        ]
      ),
      records=[
        dict(
          name="John",
          login_count=2 
        ),
        dict(
          name="Bob",
          login_count=1
        )
      ]

    )

  def test_discodb_server(self):
    dataset = DataSet()
    server = DiscoDBServer(users=self.db)

    # return all records from the users table
    results = server.evaluate(LoadOp('users'))

    assert_sequence_equal(
      list(results), 
      [
        ('John', 2),
        ('Bob',1),
      ]
    )

  def test_sample_db(self):
    sample_path = os.path.join(
      os.path.dirname(__file__),
      "sample.discodb"
    )
    dataset = DataSet()

    server = DiscoDBServer(docs=sample_path)
   
    results = list(server.evaluate(LoadOp('docs')))

    eq_(len(results), 150)
