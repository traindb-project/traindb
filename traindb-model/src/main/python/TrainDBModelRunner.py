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

class TrainDBModelRunner():

  def _load_module(self, model_class, model_path):
    spec = importlib.util.spec_from_file_location(model_class, model_path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    return mod

  def train_model(self, model_class, model_path, real_data, table_metadata, instance_path, args=[], kwargs={}):
    mod = self._load_module(model_class, model_path)
    model = getattr(mod, model_class)(*args, **kwargs)
    model.train(real_data, table_metadata)
    model.save(instance_path)

  def generate_synopsis(self, model_class, model_path, instance_path, row_count):
    mod = self._load_module(model_class, model_path)
    model = getattr(mod, model_class)()
    model.load(instance_path)
    syn_data = model.synopsis(row_count)
    return syn_data


import argparse
import pandas as pd
import json

root_parser = argparse.ArgumentParser(description='TrainDB Model Runner')
subparsers = root_parser.add_subparsers(dest='cmd')
parser_train = subparsers.add_parser('train', help='train model command')
parser_train.add_argument('model_class', type=str, help='(str) model class name')
parser_train.add_argument('model_uri', type=str, help='(str) path for local model, or uri for remote model')
parser_train.add_argument('data_file', type=str, help='(str) path to .csv data file')
parser_train.add_argument('metadata_file', type=str, help='(str) path to .json table metadata file')
parser_train.add_argument('instance_path', type=str, help='(str) path to model instance')

parser_synopsis = subparsers.add_parser('synopsis', help='generate synopsis command')
parser_synopsis.add_argument('model_class', type=str, help='(str) model class name')
parser_synopsis.add_argument('model_uri', type=str, help='(str) path for local model, or uri for remote model')
parser_synopsis.add_argument('instance_path', type=str, help='(str) path to model instance')
parser_synopsis.add_argument('row_count', type=int, help='(int) the number of rows to generate')
parser_synopsis.add_argument('output_file', type=str, help='(str) path to save generated synopsis file')

args = root_parser.parse_args()
runner = TrainDBModelRunner()
if args.cmd == 'train':
  data_file = pd.read_csv(args.data_file)
  with open(args.metadata_file) as metadata_file:
    table_metadata = json.load(metadata_file)
  runner.train_model(args.model_class, args.model_uri, data_file, table_metadata, args.instance_path)
elif args.cmd == 'synopsis':
  syn_data = runner.generate_synopsis(args.model_class, args.model_uri, args.instance_path, args.row_count)
  syn_data.to_csv(args.output_file)
else:
  print('command error')
