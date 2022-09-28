"""
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import importlib
import json
import os
import jaydebeapi
from py4j.java_gateway import JavaGateway, GatewayParameters, CallbackServerParameters
import pandas as pd

class TrainDBModelRunner(object):

  class Java:
    implements = [ "traindb.engine.TrainDBModelRunner" ]

  def init(self, java_port, python_port):
    self.gateway = JavaGateway(
        gateway_parameters = GatewayParameters(port=java_port),
        callback_server_parameters = CallbackServerParameters(port=python_port),
        python_server_entry_point = self)

  def connect(self, driver_class_name, url, user, password, jdbc_jar_path):
    self.conn = jaydebeapi.connect(
        driver_class_name, url, [ user, password ], jdbc_jar_path)

  def trainModel(self, sql_training_data, modeltype_class, modeltype_path, training_metadata, model_path, args=[], kwargs={}):
    curs = self.conn.cursor()
    curs.execute(sql_training_data)
    header = [desc[0] for desc in curs.description]
    data = pd.DataFrame(curs.fetchall(), columns=header)
    metadata = json.loads(training_metadata)

    mod = self._load_module(modeltype_class, modeltype_path)
    model = getattr(mod, modeltype_class)(*args, **metadata['options'])
    model.train(data, metadata)
    model.save(model_path)

    train_info = {}
    train_info['base_table_rows'] = len(data.index)
    train_info['trained_rows'] = len(data.index)
    return json.dumps(train_info)

  def generateSynopsis(self, modeltype_class, modeltype_path, model_path, row_count, output_file):
    mod = self._load_module(modeltype_class, modeltype_path)
    model = getattr(mod, modeltype_class)()
    model.load(model_path)
    syn_data = model.synopsis(row_count)
    syn_data.to_csv(output_file, index=False)

  def _load_module(self, modeltype_class, modeltype_path):
    spec = importlib.util.spec_from_file_location(modeltype_class, modeltype_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    return mod


import argparse

root_parser = argparse.ArgumentParser(description='TrainDB Model Runner')
root_parser.add_argument('java_port', type=int)
root_parser.add_argument('python_port', type=int)
args = root_parser.parse_args()

runner = TrainDBModelRunner()
runner.init(args.java_port, args.python_port)
